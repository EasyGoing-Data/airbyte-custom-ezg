#!/usr/bin/env bash
# =============================================================================
# build.sh — source-apple-store-custom
# Chạy trên GCP VM (nơi có Docker).
# Usage:
#   ./build.sh          → full build (check + build + test discover)
#   ./build.sh check    → syntax check only
#   ./build.sh build    → docker build only
#   ./build.sh push     → docker push only
#   ./build.sh discover → test discover in Docker (cần secrets/config.json)
#   ./build.sh read     → test read 1 ngày (cần secrets/config.json + catalog)
# =============================================================================

set -euo pipefail

# ─── Config ──────────────────────────────────────────────────────────────────
IMAGE_NAME="dataezg/source-apple-store-custom"
IMAGE_TAG="0.1.0"
IMAGE_FULL="${IMAGE_NAME}:${IMAGE_TAG}"
PYTHON="python3"

# ─── Colors ──────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()    { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error()   { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }
section() { echo -e "\n${GREEN}══ $* ══${NC}"; }

# ─── Helpers ─────────────────────────────────────────────────────────────────

step_check() {
  section "1. Syntax Check"

  info "Checking Python files..."
  for f in source_apple_store_custom/*.py main.py; do
    $PYTHON -m py_compile "$f" \
      && info "  ✅  $f" \
      || error "  ❌  $f — syntax error"
  done

  info "Checking schemas exist..."
  for schema in summary_sales financial_report app_installations app_sessions; do
    SCHEMA="source_apple_store_custom/schemas/${schema}.json"
    [ -f "$SCHEMA" ] \
      && info "  ✅  $SCHEMA" \
      || error "  ❌  $SCHEMA — MISSING (docker build sẽ thiếu schema)"
  done

  info "Checking spec.yaml exists..."
  [ -f "source_apple_store_custom/spec.yaml" ] \
    && info "  ✅  spec.yaml" \
    || error "  ❌  spec.yaml missing"

  info "Syntax check PASSED ✅"
}

step_build() {
  section "2. Docker Build"

  # Tạo fake secrets dir nếu chưa có (không cần credentials thật để build)
  mkdir -p secrets

  info "Building image: ${IMAGE_FULL}"
  docker build -t "${IMAGE_FULL}" . 2>&1 \
    | sed 's/^/  /' \
    || error "Docker build FAILED ❌"

  info "Build PASSED ✅ — image: ${IMAGE_FULL}"
}

step_verify_schemas() {
  section "3. Verify Schemas in Image"

  info "Listing schemas inside Docker image..."
  docker run --rm --entrypoint ls "${IMAGE_FULL}" \
    /airbyte/integration_code/source_apple_store_custom/schemas/ \
    | sed 's/^/  /' \
    || error "Cannot list schemas — image may be broken"

  # Đảm bảo 4 schemas đều có mặt trong image
  for schema in summary_sales.json financial_report.json app_installations.json app_sessions.json; do
    docker run --rm --entrypoint test "${IMAGE_FULL}" \
      -f "/airbyte/integration_code/source_apple_store_custom/schemas/${schema}" \
      && info "  ✅  ${schema}" \
      || error "  ❌  ${schema} MISSING inside image — kiểm tra pyproject.toml include"
  done
}

step_discover() {
  section "4. Test Discover"

  # Tạo fake config nếu chưa có
  FAKE_CONFIG="secrets/fake_config.json"
  if [ ! -f "$FAKE_CONFIG" ]; then
    warn "Không tìm thấy ${FAKE_CONFIG} — tạo fake config để test discover..."
    cat > "$FAKE_CONFIG" <<'EOF'
{
  "key_id":      "FAKE_KEY_ID",
  "issuer_id":   "00000000-0000-0000-0000-000000000000",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgFAKEKEYHERE\n-----END PRIVATE KEY-----",
  "vendors": [
    { "vendor_id": "1234567", "vendor_name": "EasyGoing VN" }
  ],
  "start_date":      "2026-01-01",
  "lookback_days":   7,
  "get_last_x_days": false,
  "timezone":        "Asia/Ho_Chi_Minh"
}
EOF
    info "Fake config tạo tại ${FAKE_CONFIG}"
  fi

  info "Running discover..."
  docker run --rm \
    -v "$(pwd)/secrets:/secrets" \
    "${IMAGE_FULL}" discover --config /secrets/fake_config.json \
    | python3 -m json.tool 2>/dev/null \
    | grep -E '"name"|"type"' \
    | sed 's/^/  /' \
    || warn "Discover có thể fail do fake credentials — kiểm tra output trên"

  info "Kết quả kỳ vọng: 4 streams (summary_sales, financial_report, app_installations, app_sessions)"
}

step_check_connection() {
  section "5. Test Connection (credentials thật)"

  CONFIG="secrets/config.json"
  if [ ! -f "$CONFIG" ]; then
    warn "Không tìm thấy ${CONFIG} — bỏ qua bước check connection."
    warn "Tạo secrets/config.json với credentials thật rồi chạy lại: ./build.sh check_conn"
    return
  fi

  info "Running check connection..."
  docker run --rm \
    -v "$(pwd)/secrets:/secrets" \
    "${IMAGE_FULL}" check --config /secrets/config.json \
    | python3 -m json.tool \
    | sed 's/^/  /'
}

step_read() {
  section "6. Test Read (credentials thật)"

  CONFIG="secrets/config.json"
  CATALOG="secrets/catalog.json"

  if [ ! -f "$CONFIG" ]; then
    warn "Không tìm thấy ${CONFIG} — bỏ qua bước read."
    return
  fi

  # Tạo catalog nếu chưa có (chỉ sync summary_sales để test nhanh)
  if [ ! -f "$CATALOG" ]; then
    info "Tạo catalog test (summary_sales only)..."
    cat > "$CATALOG" <<'EOF'
{
  "streams": [
    {
      "stream": { "name": "summary_sales", "json_schema": {} },
      "sync_mode": "incremental",
      "destination_sync_mode": "append_dedup"
    }
  ]
}
EOF
  fi

  info "Running read (summary_sales, incremental)..."
  docker run --rm \
    -v "$(pwd)/secrets:/secrets" \
    "${IMAGE_FULL}" read \
      --config  /secrets/config.json \
      --catalog /secrets/catalog.json \
    | head -50 \
    | sed 's/^/  /'
}

step_push() {
  section "7. Docker Push"

  info "Pushing ${IMAGE_FULL} to Docker Hub..."
  docker push "${IMAGE_FULL}" \
    || error "Push FAILED — đã chạy 'docker login' chưa?"

  info "Push DONE ✅"
  info ""
  info "Bước tiếp theo trên Airbyte UI:"
  info "  Settings → Sources → + New Connector"
  info "  Docker Image:  ${IMAGE_NAME}"
  info "  Docker Tag:    ${IMAGE_TAG}"
}

# ─── Main ─────────────────────────────────────────────────────────────────────

COMMAND="${1:-all}"

case "$COMMAND" in
  check)        step_check ;;
  build)        step_build; step_verify_schemas ;;
  discover)     step_discover ;;
  check_conn)   step_check_connection ;;
  read)         step_read ;;
  push)         step_push ;;
  all)
    step_check
    step_build
    step_verify_schemas
    step_discover
    step_check_connection
    echo ""
    info "══════════════════════════════════════"
    info "Build hoàn tất ✅  image: ${IMAGE_FULL}"
    info "Chạy './build.sh push' khi sẵn sàng deploy."
    info "══════════════════════════════════════"
    ;;
  *)
    echo "Usage: $0 [check|build|discover|check_conn|read|push|all]"
    exit 1
    ;;
esac
