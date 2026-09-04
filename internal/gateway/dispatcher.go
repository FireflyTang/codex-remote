package gateway

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	remotev1 "github.com/FireflyTang/codex-remote-protocol/gen/go/codex/remote/v1"
	"github.com/kylin1993/codex-remote/internal/persistence"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// Backend is the stable C/S-facing boundary. Implementations may coordinate
// directory, session, manager and adapter modules without exposing vendor wire.
type Backend interface {
	GetHost(context.Context, *remotev1.GetHostRequest) (*remotev1.GetHostResponse, error)
	ListDirectories(context.Context, *remotev1.ListDirectoriesRequest) (*remotev1.ListDirectoriesResponse, error)
	ListSessionCandidates(context.Context, *remotev1.ListSessionCandidatesRequest) (*remotev1.ListSessionCandidatesResponse, error)
	ListCodexes(context.Context, *remotev1.ListCodexesRequest) (*remotev1.ListCodexesResponse, error)
	CreateCodex(context.Context, *remotev1.CreateCodexRequest) (*remotev1.CreateCodexResponse, error)
	ImportSession(context.Context, *remotev1.ImportSessionRequest) (*remotev1.ImportSessionResponse, error)
	ListHistory(context.Context, *remotev1.ListHistoryRequest) (*remotev1.ListHistoryResponse, error)
	StartTurn(context.Context, *remotev1.StartTurnRequest) (*remotev1.StartTurnResponse, error)
	InterruptTurn(context.Context, *remotev1.InterruptTurnRequest) (*remotev1.InterruptTurnResponse, error)
	RespondApproval(context.Context, *remotev1.RespondApprovalRequest) (*remotev1.RespondApprovalResponse, error)
	RespondUserInput(context.Context, *remotev1.RespondUserInputRequest) (*remotev1.RespondUserInputResponse, error)
	UnmanageCodex(context.Context, *remotev1.UnmanageCodexRequest) (*remotev1.UnmanageCodexResponse, error)
	RenameCodex(context.Context, *remotev1.RenameCodexRequest) (*remotev1.RenameCodexResponse, error)
	ForgetCodex(context.Context, *remotev1.ForgetCodexRequest) (*remotev1.ForgetCodexResponse, error)
	GetWorkspace(context.Context, *remotev1.GetWorkspaceRequest) (*remotev1.GetWorkspaceResponse, error)
	ListWorkspaceEntries(context.Context, *remotev1.ListWorkspaceEntriesRequest) (*remotev1.ListWorkspaceEntriesResponse, error)
	ReadWorkspaceTextFile(context.Context, *remotev1.ReadWorkspaceTextFileRequest) (*remotev1.ReadWorkspaceTextFileResponse, error)
	WriteWorkspaceTextFile(context.Context, *remotev1.WriteWorkspaceTextFileRequest) (*remotev1.WriteWorkspaceTextFileResponse, error)
	UploadWorkspaceEntry(context.Context, *remotev1.UploadWorkspaceEntryRequest) (*remotev1.UploadWorkspaceEntryResponse, error)
	DownloadWorkspaceEntry(context.Context, *remotev1.DownloadWorkspaceEntryRequest) (*remotev1.DownloadWorkspaceEntryResponse, error)
	UploadImageAttachment(context.Context, *remotev1.UploadImageAttachmentRequest) (*remotev1.UploadImageAttachmentResponse, error)
	DownloadImageAttachment(context.Context, *remotev1.DownloadImageAttachmentRequest) (*remotev1.DownloadImageAttachmentResponse, error)
}

type DedupStore interface {
	BeginRequest(context.Context, string, string, []byte) (persistence.DedupResult, error)
	CompleteRequest(context.Context, string, []byte) error
}

type RPCError struct{ Detail *remotev1.Error }

func (e *RPCError) Error() string {
	if e == nil || e.Detail == nil {
		return "RPC error"
	}
	return e.Detail.Message
}

type Dispatcher struct {
	Backend Backend
	Dedup   DedupStore
}

type requestIDContextKey struct{}

func RequestIDFromContext(ctx context.Context) string {
	v, _ := ctx.Value(requestIDContextKey{}).(string)
	return v
}

func (d *Dispatcher) Dispatch(ctx context.Context, req *remotev1.Request) (*remotev1.Response, error) {
	if invalid := validateRequest(req); invalid != nil {
		return invalid, nil
	}
	if d.Backend == nil {
		return errorResponse(req.RequestId, remotev1.ErrorCode_ERROR_CODE_HOST_NOT_READY, "Host backend is not ready", true), nil
	}
	ctx = context.WithValue(ctx, requestIDContextKey{}, req.RequestId)
	op, sideEffect := operation(req)
	if op == "watch_codex" || op == "unwatch_codex" {
		return nil, fmt.Errorf("%s is connection-scoped", op)
	}
	if !sideEffect {
		return d.call(ctx, req)
	}
	if d.Dedup == nil {
		return errorResponse(req.RequestId, remotev1.ErrorCode_ERROR_CODE_HOST_NOT_READY, "request deduplication is unavailable", true), nil
	}
	fingerprintRequest := proto.Clone(req).(*remotev1.Request)
	fingerprintRequest.RequestId = ""
	fingerprintRequest.SentAtUnixMs = 0
	fingerprintRequest.DeadlineUnixMs = 0
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(fingerprintRequest)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(payload)
	state, err := d.Dedup.BeginRequest(ctx, req.RequestId, op, sum[:])
	if errors.Is(err, persistence.ErrRequestConflict) {
		return errorResponse(req.RequestId, remotev1.ErrorCode_ERROR_CODE_CONFLICT, "request_id was already used with different payload", false), nil
	}
	if errors.Is(err, persistence.ErrRequestInProgress) {
		return errorResponse(req.RequestId, remotev1.ErrorCode_ERROR_CODE_CONFLICT, "request outcome is unknown; IN_PROGRESS operation will not be replayed", false), nil
	}
	if err != nil {
		return errorResponse(req.RequestId, remotev1.ErrorCode_ERROR_CODE_INTERNAL_ERROR, err.Error(), true), nil
	}
	if state.State == persistence.DedupCompleted {
		resp := new(remotev1.Response)
		if err := protojson.Unmarshal(state.ResponseJSON, resp); err != nil {
			return nil, err
		}
		markDeduplicated(resp)
		return resp, nil
	}
	resp, err := d.call(ctx, req)
	if err != nil {
		return nil, err
	}
	raw, err := protojson.Marshal(resp)
	if err != nil {
		return nil, err
	}
	if err = d.Dedup.CompleteRequest(ctx, req.RequestId, raw); err != nil {
		return nil, err
	}
	return resp, nil
}

// validateRequest is shared by backend RPCs and the connection-scoped
// Watch/Unwatch paths so envelope and deadline behavior cannot drift.
func validateRequest(req *remotev1.Request) *remotev1.Response {
	if req == nil || req.RequestId == "" {
		return errorResponse(requestID(req), remotev1.ErrorCode_ERROR_CODE_INVALID_REQUEST, "request_id is required", false)
	}
	if req.Request == nil {
		return errorResponse(req.RequestId, remotev1.ErrorCode_ERROR_CODE_INVALID_REQUEST, "request payload is required", false)
	}
	if req.DeadlineUnixMs < 0 || req.DeadlineUnixMs > 0 && time.Now().UnixMilli() >= req.DeadlineUnixMs {
		return errorResponse(req.RequestId, remotev1.ErrorCode_ERROR_CODE_DEADLINE_EXCEEDED, "request deadline has elapsed", true)
	}
	return nil
}

func (d *Dispatcher) call(ctx context.Context, req *remotev1.Request) (*remotev1.Response, error) {
	resp := &remotev1.Response{RequestId: req.RequestId, RespondedAtUnixMs: time.Now().UnixMilli()}
	var err error
	switch x := req.Request.(type) {
	case *remotev1.Request_GetHost:
		var v *remotev1.GetHostResponse
		v, err = d.Backend.GetHost(ctx, x.GetHost)
		resp.Result = &remotev1.Response_GetHost{GetHost: v}
	case *remotev1.Request_ListDirectories:
		var v *remotev1.ListDirectoriesResponse
		v, err = d.Backend.ListDirectories(ctx, x.ListDirectories)
		resp.Result = &remotev1.Response_ListDirectories{ListDirectories: v}
	case *remotev1.Request_ListSessionCandidates:
		var v *remotev1.ListSessionCandidatesResponse
		v, err = d.Backend.ListSessionCandidates(ctx, x.ListSessionCandidates)
		resp.Result = &remotev1.Response_ListSessionCandidates{ListSessionCandidates: v}
	case *remotev1.Request_ListCodexes:
		var v *remotev1.ListCodexesResponse
		v, err = d.Backend.ListCodexes(ctx, x.ListCodexes)
		resp.Result = &remotev1.Response_ListCodexes{ListCodexes: v}
	case *remotev1.Request_CreateCodex:
		var v *remotev1.CreateCodexResponse
		v, err = d.Backend.CreateCodex(ctx, x.CreateCodex)
		resp.Result = &remotev1.Response_CreateCodex{CreateCodex: v}
	case *remotev1.Request_ImportSession:
		var v *remotev1.ImportSessionResponse
		v, err = d.Backend.ImportSession(ctx, x.ImportSession)
		resp.Result = &remotev1.Response_ImportSession{ImportSession: v}
	case *remotev1.Request_ListHistory:
		var v *remotev1.ListHistoryResponse
		v, err = d.Backend.ListHistory(ctx, x.ListHistory)
		resp.Result = &remotev1.Response_ListHistory{ListHistory: v}
	case *remotev1.Request_StartTurn:
		var v *remotev1.StartTurnResponse
		v, err = d.Backend.StartTurn(ctx, x.StartTurn)
		resp.Result = &remotev1.Response_StartTurn{StartTurn: v}
	case *remotev1.Request_InterruptTurn:
		var v *remotev1.InterruptTurnResponse
		v, err = d.Backend.InterruptTurn(ctx, x.InterruptTurn)
		resp.Result = &remotev1.Response_InterruptTurn{InterruptTurn: v}
	case *remotev1.Request_RespondApproval:
		var v *remotev1.RespondApprovalResponse
		v, err = d.Backend.RespondApproval(ctx, x.RespondApproval)
		resp.Result = &remotev1.Response_RespondApproval{RespondApproval: v}
	case *remotev1.Request_RespondUserInput:
		var v *remotev1.RespondUserInputResponse
		v, err = d.Backend.RespondUserInput(ctx, x.RespondUserInput)
		resp.Result = &remotev1.Response_RespondUserInput{RespondUserInput: v}
	case *remotev1.Request_UnmanageCodex:
		var v *remotev1.UnmanageCodexResponse
		v, err = d.Backend.UnmanageCodex(ctx, x.UnmanageCodex)
		resp.Result = &remotev1.Response_UnmanageCodex{UnmanageCodex: v}
	case *remotev1.Request_RenameCodex:
		var v *remotev1.RenameCodexResponse
		v, err = d.Backend.RenameCodex(ctx, x.RenameCodex)
		resp.Result = &remotev1.Response_RenameCodex{RenameCodex: v}
	case *remotev1.Request_ForgetCodex:
		var v *remotev1.ForgetCodexResponse
		v, err = d.Backend.ForgetCodex(ctx, x.ForgetCodex)
		resp.Result = &remotev1.Response_ForgetCodex{ForgetCodex: v}
	case *remotev1.Request_GetWorkspace:
		var v *remotev1.GetWorkspaceResponse
		v, err = d.Backend.GetWorkspace(ctx, x.GetWorkspace)
		resp.Result = &remotev1.Response_GetWorkspace{GetWorkspace: v}
	case *remotev1.Request_ListWorkspaceEntries:
		var v *remotev1.ListWorkspaceEntriesResponse
		v, err = d.Backend.ListWorkspaceEntries(ctx, x.ListWorkspaceEntries)
		resp.Result = &remotev1.Response_ListWorkspaceEntries{ListWorkspaceEntries: v}
	case *remotev1.Request_ReadWorkspaceTextFile:
		var v *remotev1.ReadWorkspaceTextFileResponse
		v, err = d.Backend.ReadWorkspaceTextFile(ctx, x.ReadWorkspaceTextFile)
		resp.Result = &remotev1.Response_ReadWorkspaceTextFile{ReadWorkspaceTextFile: v}
	case *remotev1.Request_WriteWorkspaceTextFile:
		var v *remotev1.WriteWorkspaceTextFileResponse
		v, err = d.Backend.WriteWorkspaceTextFile(ctx, x.WriteWorkspaceTextFile)
		resp.Result = &remotev1.Response_WriteWorkspaceTextFile{WriteWorkspaceTextFile: v}
	case *remotev1.Request_UploadWorkspaceEntry:
		var v *remotev1.UploadWorkspaceEntryResponse
		v, err = d.Backend.UploadWorkspaceEntry(ctx, x.UploadWorkspaceEntry)
		resp.Result = &remotev1.Response_UploadWorkspaceEntry{UploadWorkspaceEntry: v}
	case *remotev1.Request_DownloadWorkspaceEntry:
		var v *remotev1.DownloadWorkspaceEntryResponse
		v, err = d.Backend.DownloadWorkspaceEntry(ctx, x.DownloadWorkspaceEntry)
		resp.Result = &remotev1.Response_DownloadWorkspaceEntry{DownloadWorkspaceEntry: v}
	case *remotev1.Request_UploadImageAttachment:
		var v *remotev1.UploadImageAttachmentResponse
		v, err = d.Backend.UploadImageAttachment(ctx, x.UploadImageAttachment)
		resp.Result = &remotev1.Response_UploadImageAttachment{UploadImageAttachment: v}
	case *remotev1.Request_DownloadImageAttachment:
		var v *remotev1.DownloadImageAttachmentResponse
		v, err = d.Backend.DownloadImageAttachment(ctx, x.DownloadImageAttachment)
		resp.Result = &remotev1.Response_DownloadImageAttachment{DownloadImageAttachment: v}
	default:
		return errorResponse(req.RequestId, remotev1.ErrorCode_ERROR_CODE_INVALID_REQUEST, "unsupported request", false), nil
	}
	if err == nil {
		return resp, nil
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
		resp.Result = &remotev1.Response_Error{Error: &remotev1.Error{Code: remotev1.ErrorCode_ERROR_CODE_DEADLINE_EXCEEDED, Message: "request deadline has elapsed", Retryable: true}}
		return resp, nil
	}
	var rpc *RPCError
	if errors.As(err, &rpc) && rpc.Detail != nil {
		resp.Result = &remotev1.Response_Error{Error: rpc.Detail}
	} else {
		resp.Result = &remotev1.Response_Error{Error: &remotev1.Error{Code: remotev1.ErrorCode_ERROR_CODE_INTERNAL_ERROR, Message: err.Error(), Retryable: true}}
	}
	return resp, nil
}

func operation(req *remotev1.Request) (string, bool) {
	switch req.Request.(type) {
	case *remotev1.Request_GetHost:
		return "get_host", false
	case *remotev1.Request_ListDirectories:
		return "list_directories", false
	case *remotev1.Request_ListSessionCandidates:
		return "list_session_candidates", false
	case *remotev1.Request_ListCodexes:
		return "list_codexes", false
	case *remotev1.Request_CreateCodex:
		return "create_codex", true
	case *remotev1.Request_ImportSession:
		return "import_session", true
	case *remotev1.Request_WatchCodex:
		return "watch_codex", false
	case *remotev1.Request_UnwatchCodex:
		return "unwatch_codex", false
	case *remotev1.Request_ListHistory:
		return "list_history", false
	case *remotev1.Request_StartTurn:
		return "start_turn", true
	case *remotev1.Request_InterruptTurn:
		return "interrupt_turn", true
	case *remotev1.Request_RespondApproval:
		return "respond_approval", true
	case *remotev1.Request_RespondUserInput:
		return "respond_user_input", true
	case *remotev1.Request_UnmanageCodex:
		return "unmanage_codex", true
	case *remotev1.Request_RenameCodex:
		return "rename_codex", true
	case *remotev1.Request_ForgetCodex:
		return "forget_codex", true
	case *remotev1.Request_GetWorkspace:
		return "get_workspace", false
	case *remotev1.Request_ListWorkspaceEntries:
		return "list_workspace_entries", false
	case *remotev1.Request_ReadWorkspaceTextFile:
		return "read_workspace_text_file", false
	case *remotev1.Request_WriteWorkspaceTextFile:
		return "write_workspace_text_file", true
	case *remotev1.Request_UploadWorkspaceEntry:
		return "upload_workspace_entry", true
	case *remotev1.Request_DownloadWorkspaceEntry:
		return "download_workspace_entry", false
	case *remotev1.Request_UploadImageAttachment:
		return "upload_image_attachment", true
	case *remotev1.Request_DownloadImageAttachment:
		return "download_image_attachment", false
	default:
		return "unknown", false
	}
}
func errorResponse(id string, code remotev1.ErrorCode, message string, retry bool) *remotev1.Response {
	return &remotev1.Response{RequestId: id, RespondedAtUnixMs: time.Now().UnixMilli(), Result: &remotev1.Response_Error{Error: &remotev1.Error{Code: code, Message: message, Retryable: retry}}}
}
func requestID(req *remotev1.Request) string {
	if req == nil {
		return ""
	}
	return req.RequestId
}
func markDeduplicated(r *remotev1.Response) {
	switch x := r.Result.(type) {
	case *remotev1.Response_CreateCodex:
		x.CreateCodex.Deduplicated = true
	case *remotev1.Response_ImportSession:
		x.ImportSession.Deduplicated = true
	case *remotev1.Response_StartTurn:
		x.StartTurn.Deduplicated = true
	case *remotev1.Response_InterruptTurn:
		x.InterruptTurn.Deduplicated = true
	case *remotev1.Response_RespondApproval:
		x.RespondApproval.Deduplicated = true
	case *remotev1.Response_RespondUserInput:
		x.RespondUserInput.Deduplicated = true
	case *remotev1.Response_UnmanageCodex:
		x.UnmanageCodex.Deduplicated = true
	case *remotev1.Response_RenameCodex:
		x.RenameCodex.Deduplicated = true
	case *remotev1.Response_ForgetCodex:
		x.ForgetCodex.Deduplicated = true
	case *remotev1.Response_UploadImageAttachment:
		x.UploadImageAttachment.Deduplicated = true
	case *remotev1.Response_WriteWorkspaceTextFile:
		x.WriteWorkspaceTextFile.Deduplicated = true
	case *remotev1.Response_UploadWorkspaceEntry:
		x.UploadWorkspaceEntry.Deduplicated = true
	}
}
