// Package attachment persists and validates immutable image attachments.
package attachment

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/kylin1993/codex-remote/internal/persistence"
)

var (
	ErrNotFound        = errors.New("image attachment not found")
	ErrTooLarge        = errors.New("image attachment is too large")
	ErrMIMEUnsupported = errors.New("image attachment MIME type is unsupported")
	ErrContentInvalid  = errors.New("image attachment content is invalid")
	ErrHashMismatch    = errors.New("image attachment hash mismatch")
)

const DefaultMaxUploadBytes uint64 = 8 << 20
const MaxFilenameBytes = 1024

var DefaultSupportedMIMETypes = []string{"image/gif", "image/jpeg", "image/png"}

type Config struct {
	MaxUploadBytes     uint64
	SupportedMIMETypes []string
}

type Descriptor struct {
	AttachmentID              string
	Filename                  string
	MIMEType                  string
	SizeBytes                 uint64
	SHA256                    string
	WidthPixels, HeightPixels uint32
}

type MetadataStore interface {
	SaveAttachment(context.Context, persistence.AttachmentRecord) error
	GetAttachment(context.Context, string, string) (persistence.AttachmentRecord, error)
}

type Store struct {
	root, blobsDir string
	metadata       MetadataStore
	maxUploadBytes uint64
	supportedMIMEs []string
}

func DefaultConfig() Config {
	return Config{MaxUploadBytes: DefaultMaxUploadBytes, SupportedMIMETypes: slices.Clone(DefaultSupportedMIMETypes)}
}

func New(root string, metadata MetadataStore, cfg Config) (*Store, error) {
	if strings.TrimSpace(root) == "" || metadata == nil {
		return nil, errors.New("attachment root and metadata store are required")
	}
	if cfg.MaxUploadBytes == 0 || len(cfg.SupportedMIMETypes) == 0 {
		return nil, errors.New("positive max upload bytes and supported MIME types are required")
	}
	seen := make(map[string]bool, len(cfg.SupportedMIMETypes))
	for _, mimeType := range cfg.SupportedMIMETypes {
		if canonicalMIMEFormat(mimeType) == "" || seen[mimeType] {
			return nil, fmt.Errorf("unsupported configured image MIME type %q", mimeType)
		}
		seen[mimeType] = true
	}
	root, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	blobsDir := filepath.Join(root, "blobs")
	if err := os.MkdirAll(blobsDir, 0o700); err != nil {
		return nil, err
	}
	return &Store{root: root, blobsDir: blobsDir, metadata: metadata, maxUploadBytes: cfg.MaxUploadBytes, supportedMIMEs: slices.Clone(cfg.SupportedMIMETypes)}, nil
}

func (s *Store) Upload(ctx context.Context, ownerID, filename, mimeType, wantSHA string, content []byte) (Descriptor, error) {
	if strings.TrimSpace(ownerID) == "" || filename == "" || len(filename) > MaxFilenameBytes || !utf8.ValidString(filename) || strings.IndexByte(filename, 0) >= 0 {
		return Descriptor{}, fmt.Errorf("%w: owner and display filename are required", ErrContentInvalid)
	}
	if !slices.Contains(s.supportedMIMEs, mimeType) {
		return Descriptor{}, fmt.Errorf("%w: %q", ErrMIMEUnsupported, mimeType)
	}
	if !validSHA256.MatchString(wantSHA) {
		return Descriptor{}, fmt.Errorf("%w: sha256 must be 64 lowercase hexadecimal digits", ErrContentInvalid)
	}
	if len(content) == 0 {
		return Descriptor{}, fmt.Errorf("%w: empty image", ErrContentInvalid)
	}
	if uint64(len(content)) > s.maxUploadBytes {
		return Descriptor{}, ErrTooLarge
	}
	actualHash := sha256.Sum256(content)
	if hex.EncodeToString(actualHash[:]) != wantSHA {
		return Descriptor{}, ErrHashMismatch
	}
	imageConfig, format, err := image.DecodeConfig(bytes.NewReader(content))
	if err != nil || imageConfig.Width <= 0 || imageConfig.Height <= 0 || format != canonicalMIMEFormat(mimeType) || uint64(imageConfig.Width) > uint64(^uint32(0)) || uint64(imageConfig.Height) > uint64(^uint32(0)) {
		return Descriptor{}, fmt.Errorf("%w: declared MIME and decoded image disagree", ErrContentInvalid)
	}
	attachmentID, err := opaqueID()
	if err != nil {
		return Descriptor{}, err
	}
	d := Descriptor{AttachmentID: attachmentID, Filename: filename, MIMEType: mimeType, SizeBytes: uint64(len(content)), SHA256: wantSHA, WidthPixels: uint32(imageConfig.Width), HeightPixels: uint32(imageConfig.Height)}
	path := s.blobPath(attachmentID)
	if err := writeAtomic(path, content); err != nil {
		return Descriptor{}, err
	}
	record := persistence.AttachmentRecord{AttachmentID: d.AttachmentID, LogicalOwnerID: ownerID, Filename: d.Filename, MIMEType: d.MIMEType, SizeBytes: d.SizeBytes, SHA256: d.SHA256, WidthPixels: d.WidthPixels, HeightPixels: d.HeightPixels, CreatedAtUnixMS: time.Now().UnixMilli()}
	if err := s.metadata.SaveAttachment(ctx, record); err != nil {
		_ = os.Remove(path)
		return Descriptor{}, err
	}
	return d, nil
}

func (s *Store) Download(ctx context.Context, ownerID, attachmentID string) (Descriptor, []byte, error) {
	d, path, err := s.resolve(ctx, ownerID, attachmentID)
	if err != nil {
		return Descriptor{}, nil, err
	}
	content, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Descriptor{}, nil, ErrNotFound
		}
		return Descriptor{}, nil, err
	}
	if err := validatePersisted(d, content); err != nil {
		return Descriptor{}, nil, err
	}
	return d, content, nil
}

// Resolve validates an attachment for StartTurn and returns its private Host path.
func (s *Store) Resolve(ctx context.Context, ownerID, attachmentID string) (Descriptor, string, error) {
	d, content, err := s.Download(ctx, ownerID, attachmentID)
	if err != nil {
		return Descriptor{}, "", err
	}
	_ = content
	return d, s.blobPath(attachmentID), nil
}

// DescribePath maps an app-server localImage path back to its descriptor.
func (s *Store) DescribePath(ctx context.Context, ownerID, path string) (Descriptor, error) {
	abs, err := filepath.Abs(path)
	if err != nil || filepath.Dir(abs) != s.blobsDir {
		return Descriptor{}, ErrNotFound
	}
	attachmentID := filepath.Base(abs)
	if !validAttachmentID.MatchString(attachmentID) {
		return Descriptor{}, ErrNotFound
	}
	d, _, err := s.Resolve(ctx, ownerID, attachmentID)
	return d, err
}

func (s *Store) resolve(ctx context.Context, ownerID, attachmentID string) (Descriptor, string, error) {
	if strings.TrimSpace(ownerID) == "" || !validAttachmentID.MatchString(attachmentID) {
		return Descriptor{}, "", ErrNotFound
	}
	r, err := s.metadata.GetAttachment(ctx, ownerID, attachmentID)
	if errors.Is(err, sql.ErrNoRows) {
		return Descriptor{}, "", ErrNotFound
	}
	if err != nil {
		return Descriptor{}, "", err
	}
	d := descriptorFromRecord(r)
	return d, s.blobPath(attachmentID), nil
}

func descriptorFromRecord(r persistence.AttachmentRecord) Descriptor {
	return Descriptor{AttachmentID: r.AttachmentID, Filename: r.Filename, MIMEType: r.MIMEType, SizeBytes: r.SizeBytes, SHA256: r.SHA256, WidthPixels: r.WidthPixels, HeightPixels: r.HeightPixels}
}

func validatePersisted(d Descriptor, content []byte) error {
	if uint64(len(content)) != d.SizeBytes {
		return ErrHashMismatch
	}
	hash := sha256.Sum256(content)
	if hex.EncodeToString(hash[:]) != d.SHA256 {
		return ErrHashMismatch
	}
	return nil
}

func (s *Store) blobPath(attachmentID string) string { return filepath.Join(s.blobsDir, attachmentID) }

var (
	validSHA256       = regexp.MustCompile(`^[0-9a-f]{64}$`)
	validAttachmentID = regexp.MustCompile(`^[0-9a-f]{32}$`)
)

func canonicalMIMEFormat(mimeType string) string {
	switch mimeType {
	case "image/png":
		return "png"
	case "image/jpeg":
		return "jpeg"
	case "image/gif":
		return "gif"
	default:
		return ""
	}
}

func opaqueID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw[:]), nil
}

func writeAtomic(path string, content []byte) (err error) {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".attachment-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() {
		_ = tmp.Close()
		if err != nil {
			_ = os.Remove(tmpName)
		}
	}()
	if err = tmp.Chmod(0o600); err != nil {
		return err
	}
	if _, err = tmp.Write(content); err != nil {
		return err
	}
	if err = tmp.Sync(); err != nil {
		return err
	}
	if err = tmp.Close(); err != nil {
		return err
	}
	if _, err = os.Lstat(path); err == nil {
		return errors.New("attachment id collision")
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err = os.Rename(tmpName, path); err != nil {
		return err
	}
	dirHandle, openErr := os.Open(dir)
	if openErr != nil {
		return openErr
	}
	err = dirHandle.Sync()
	if closeErr := dirHandle.Close(); err == nil {
		err = closeErr
	}
	return err
}
