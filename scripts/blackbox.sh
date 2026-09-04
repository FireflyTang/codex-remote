#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
go_bin="${repo_dir}/.tools/go/bin/go"
tmp_dir="$(mktemp -d)"
host_pid=""

cleanup() {
  if [[ -n "${host_pid}" ]] && kill -0 "${host_pid}" 2>/dev/null; then
    kill -TERM "${host_pid}" 2>/dev/null || true
    wait "${host_pid}" 2>/dev/null || true
  fi
  rm -rf "${tmp_dir}"
}
trap cleanup EXIT INT TERM

if [[ ! -x "${go_bin}" ]]; then
  make -C "${repo_dir}" tools
fi

mkdir -p "${repo_dir}/.tools/bin"
"${go_bin}" build -o "${repo_dir}/.tools/bin/codex-remote-fake-app-server" "${repo_dir}/testdata/fake_app_server"
"${go_bin}" build -o "${repo_dir}/.tools/bin/codex-remote-host" "${repo_dir}/cmd/codex-remote-host"

start_host() {
  local scenario="$1"
  local run_dir="${tmp_dir}/${scenario}"
  mkdir -p "${run_dir}"
  local log_file="${run_dir}/host.log"
  local max_frame_bytes=$((4 * 1024 * 1024))
  local send_queue=32
  local watch_queue=16
  local connection_timeout=2s
  local replay_capacity=32
  local lease_duration=2h
  local lease_warning_before=30m
  local lease_sweep_interval=1m
  if [[ "${scenario}" == "large" || "${scenario}" == "multi-large" || "${scenario}" == "early-large" ]]; then
    max_frame_bytes=$((64 * 1024))
  elif [[ "${scenario}" == "burst" ]]; then
    send_queue=1
    watch_queue=8
    connection_timeout=30s
    replay_capacity=1024
  elif [[ "${scenario}" == "lifecycle" ]]; then
    lease_duration=2s
    lease_warning_before=500ms
    lease_sweep_interval=25ms
  elif [[ "${scenario}" == "restart" ]]; then
    lease_duration=3s
    lease_warning_before=750ms
    lease_sweep_interval=25ms
  fi
  "${repo_dir}/.tools/bin/codex-remote-host" \
    --dev-listen 127.0.0.1:0 \
    --state-dir "${run_dir}/state" \
    --audit-dir "${run_dir}/audit" \
    --app-server-executable "${repo_dir}/.tools/bin/codex-remote-fake-app-server" \
    --app-server-arg=-scenario \
    --app-server-arg="${scenario}" \
    --app-server-arg=-state-file \
    --app-server-arg="${run_dir}/fake-state.json" \
    --heartbeat 200ms \
    --connection-timeout "${connection_timeout}" \
    --send-queue "${send_queue}" \
    --watch-queue "${watch_queue}" \
    --max-frame-bytes "${max_frame_bytes}" \
    --replay-capacity "${replay_capacity}" \
    --lease-duration "${lease_duration}" \
    --lease-warning-before "${lease_warning_before}" \
    --lease-sweep-interval "${lease_sweep_interval}" \
    --max-page-size 3 \
    >"${log_file}" 2>&1 &
  host_pid=$!

  local listen_url=""
  for _ in $(seq 1 200); do
    if ! kill -0 "${host_pid}" 2>/dev/null; then
      sed -n '1,240p' "${log_file}" >&2
      return 1
    fi
    listen_url="$(sed -n 's/.*LISTEN_URL=//p' "${log_file}" | tail -1)"
    [[ -n "${listen_url}" ]] && break
    sleep 0.05
  done
  if [[ -z "${listen_url}" ]]; then
    sed -n '1,240p' "${log_file}" >&2
    echo "Host did not publish LISTEN_URL" >&2
    return 1
  fi
  local health_url="${listen_url/ws:\/\//http://}"
  health_url="${health_url%/connect}/healthz"
  for _ in $(seq 1 100); do
    if curl --noproxy '*' -fsS "${health_url}" >/dev/null 2>&1; then
      break
    fi
    sleep 0.05
  done
  export CODEX_REMOTE_BLACKBOX_URL="${listen_url}"
  export CODEX_REMOTE_BLACKBOX_SCENARIO="${scenario}"
  export CODEX_REMOTE_BLACKBOX_STATE_DIR="${run_dir}/state"
  export CODEX_REMOTE_BLACKBOX_AUDIT_DIR="${run_dir}/audit"
  export CODEX_REMOTE_BLACKBOX_HOST_PID="${host_pid}"
  export CODEX_REMOTE_BLACKBOX_HOST_LOG="${log_file}"
}

stop_host() {
  kill -TERM "${host_pid}"
  wait "${host_pid}"
  host_pid=""
}

result=0
scenario_list="${CODEX_REMOTE_BLACKBOX_SCENARIOS:-normal workspace interrupt approval user-input sessions unmaterialized-history unmaterialized-lifecycle lifecycle rename-forget image-attachments structured failed rewatch synthetic-upsert early-large large multi-large burst audit-failure runtime-disconnect}"
for scenario in ${scenario_list}; do
  start_host "${scenario}"
  run_pattern='.'
  if [[ "${scenario}" == "large" ]]; then
    run_pattern='^TestLargeVendorOutputIsExplicitlyBounded$'
  elif [[ "${scenario}" == "multi-large" ]]; then
    run_pattern='^TestMultipleLargeItemsBoundCollectionsAndKeepConnectionUsable$'
  elif [[ "${scenario}" == "burst" ]]; then
    run_pattern='^TestSlowConsumerGetsExplicitProtocolClose$'
  elif [[ "${scenario}" == "sessions" ]]; then
    run_pattern='^Test(DiscoverImportAndContinueUnmanagedSession|PageTokensAreBoundToOperationAndNormalizedQuery)$'
  elif [[ "${scenario}" == "unmaterialized-history" ]]; then
    run_pattern='^TestUnmaterializedCreatedThreadHasEmptyHistoryUntilFirstUserMessage$'
  elif [[ "${scenario}" == "unmaterialized-lifecycle" ]]; then
    run_pattern='^TestUnmaterializedUnmanagedCodexMaterializesOnStartTurn$'
  elif [[ "${scenario}" == "lifecycle" ]]; then
    run_pattern='^Test(ManagementLifecycleOverFormalWire|HandshakeV120AndGetHost|HandshakeRejectsUnsupportedVersions)$'
  elif [[ "${scenario}" == "rename-forget" ]]; then
    run_pattern='^TestRenameAndForgetOverFormalWire$'
  elif [[ "${scenario}" == "image-attachments" ]]; then
    export CODEX_REMOTE_BLACKBOX_PHASE=create
    export CODEX_REMOTE_BLACKBOX_CHECKPOINT="${tmp_dir}/image-attachments/checkpoint.json"
    image_create_ok=true
    if ! "${go_bin}" test -count=1 -v -run '^TestImageAttachmentRestart' ./tests/blackbox; then
      result=1
      image_create_ok=false
    fi
    stop_host
    if [[ "${image_create_ok}" != true ]]; then
      unset CODEX_REMOTE_BLACKBOX_PHASE CODEX_REMOTE_BLACKBOX_CHECKPOINT
      continue
    fi
    start_host "${scenario}"
    export CODEX_REMOTE_BLACKBOX_PHASE=verify
    if ! "${go_bin}" test -count=1 -v -run '^TestImageAttachmentRestart' ./tests/blackbox; then result=1; fi
    stop_host
    unset CODEX_REMOTE_BLACKBOX_PHASE CODEX_REMOTE_BLACKBOX_CHECKPOINT
    continue
  elif [[ "${scenario}" == "workspace" ]]; then
    run_pattern='^TestWorkspace(FormalWireScenario|ListingIsShallowAndDoesNotBlockUnrelatedRPCs)$'
  elif [[ "${scenario}" == "structured" ]]; then
    run_pattern='^TestStructuredItemsDeltasAndHistory$'
  elif [[ "${scenario}" == "failed" ]]; then
    run_pattern='^TestFailedTurnPreservesStatusErrorAndTime$'
  elif [[ "${scenario}" == "rewatch" ]]; then
    run_pattern='^TestRewatchResponsePrecedesReplacementStream$'
  elif [[ "${scenario}" == "synthetic-upsert" ]]; then
    run_pattern='^TestSyntheticPlanDiffIDsAreStableAndUpserted$'
  elif [[ "${scenario}" == "early-large" ]]; then
    run_pattern='^TestEarlyLargeUpdatesSurviveStartResponseAndRemainActionable$'
  elif [[ "${scenario}" == "runtime-disconnect" ]]; then
    run_pattern='^TestRuntimeRecoversAfterAppServerSocketDisconnect$'
  elif [[ "${scenario}" == "audit-failure" ]]; then
    run_pattern='^TestAuditWriteFailureDoesNotBlockBusiness$'
  fi
  if ! "${go_bin}" test -count=1 -v -run "${run_pattern}" ./tests/blackbox; then
    result=1
  fi
  stop_host
done

if [[ -n "${CODEX_REMOTE_BLACKBOX_SCENARIOS:-}" ]]; then
  exit "${result}"
fi

start_host restart
export CODEX_REMOTE_BLACKBOX_PHASE=create
export CODEX_REMOTE_BLACKBOX_CHECKPOINT="${tmp_dir}/restart/checkpoint.json"
if ! "${go_bin}" test -count=1 -v -run '^TestRestart' ./tests/blackbox; then result=1; fi
stop_host

start_host restart
export CODEX_REMOTE_BLACKBOX_PHASE=verify
export CODEX_REMOTE_BLACKBOX_CHECKPOINT="${tmp_dir}/restart/checkpoint.json"
if ! "${go_bin}" test -count=1 -v -run '^TestRestart' ./tests/blackbox; then result=1; fi
stop_host

start_host pending-restart
export CODEX_REMOTE_BLACKBOX_PHASE=create
export CODEX_REMOTE_BLACKBOX_CHECKPOINT="${tmp_dir}/pending-restart/checkpoint.json"
if ! "${go_bin}" test -count=1 -v -run '^TestPendingRestart' ./tests/blackbox; then result=1; fi
stop_host

start_host pending-restart
export CODEX_REMOTE_BLACKBOX_PHASE=verify
export CODEX_REMOTE_BLACKBOX_CHECKPOINT="${tmp_dir}/pending-restart/checkpoint.json"
if ! "${go_bin}" test -count=1 -v -run '^TestPendingRestart' ./tests/blackbox; then result=1; fi
stop_host

exit "${result}"
