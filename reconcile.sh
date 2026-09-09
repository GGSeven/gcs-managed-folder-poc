#!/usr/bin/env bash
# ==============================================================================
# 纯 Bash + curl 高性能 GCS Managed Folder 调和脚本
# ==============================================================================
set -euo pipefail
cd "$(dirname "$0")"

if [[ -f "./config.env" ]]; then
  source ./config.env
fi

TODAY_EPOCH=$(date -u +%s)
BUCKET_NAME="${BUCKET#gs://}"
API="https://storage.googleapis.com/storage/v1/b/${BUCKET_NAME}"

# 自动获取 Token：优先从 Cloud Run 元数据服务获取，失败则回退到 gcloud
get_token() {
  local t=""
  if t=$(curl -s -m 2 -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token" 2>/dev/null | grep -o '"access_token": *"[^"]*"' | cut -d'"' -f4) && [[ -n "${t}" ]]; then
    echo "${t}"
  else
    gcloud auth print-access-token
  fi
}

TOKEN=$(get_token)
AUTH_HEADER="Authorization: Bearer ${TOKEN}"

urlenc() {
  python3 -c 'import sys,urllib.parse; print(urllib.parse.quote(sys.argv[1], safe=""))' "$1"
}

parse_folder_epoch() {
  local clean="${1#dt=}"
  date -u -d "${clean}" +%s 2>/dev/null || echo ""
}

# 1. 确保 Managed Folder 存在（毫秒级）
ensure_managed_folder() {
  local rel="$1"
  curl -s -o /dev/null -X POST \
    -H "${AUTH_HEADER}" -H "Content-Type: application/json" \
    -d "{\"name\": \"${rel}\"}" "${API}/managedFolders"
}

# 2. 设置天级分区权限（毫秒级）
apply_policy() {
  local rel="$1" sa="$2" role="$3" tier="$4"
  local mf_url="${API}/managedFolders/$(urlenc "${rel}")/iam"
  
  curl -s -o /dev/null -X PUT \
    -H "${AUTH_HEADER}" -H "Content-Type: application/json" \
    -d "{\"bindings\": [{\"role\": \"${role}\", \"members\": [\"serviceAccount:${sa}\"]}]}" \
    "${mf_url}"
  echo "      [${tier}] ${rel}"
}

# 3. 设置 metadata 读取权限（毫秒级）
apply_metadata_policy() {
  local rel="$1"
  local mf_url="${API}/managedFolders/$(urlenc "${rel}")/iam"
  
  curl -s -o /dev/null -X PUT \
    -H "${AUTH_HEADER}" -H "Content-Type: application/json" \
    -d "{\"bindings\": [{\"role\": \"roles/storage.objectViewer\", \"members\": [\"serviceAccount:${HOT_SA}\", \"serviceAccount:${COLD_SA}\"]}]}" \
    "${mf_url}"
  echo "      [METADATA OK] ${rel}"
}

# 4. 快速列出目录（使用 delimiter，不枚举底层海量数据文件）
list_sub_folders() {
  local prefix="$1"
  local page_token="" resp
  while :; do
    resp=$(curl -sf -H "${AUTH_HEADER}" \
      "${API}/o?prefix=${prefix}&delimiter=/&fields=prefixes,nextPageToken${page_token:+&pageToken=${page_token}}")
    echo "${resp}" | python3 -c 'import json,sys; [print(p) for p in json.load(sys.stdin).get("prefixes",[])]'
    page_token=$(echo "${resp}" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("nextPageToken",""))')
    [[ -z "${page_token}" ]] && break
  done
}

echo "==> 开始全库全表权限调和 (curl 极速模式)..."

for db in $(list_sub_folders "${DATA_ROOT_PREFIX}/"); do
  [[ "${db}" != *".db/"* ]] && continue
  echo "  📁 数据库: ${db}"

  for tbl in $(list_sub_folders "${db}"); do
    echo "    📊 数据表: ${tbl}"

    # 1. 授权 metadata
    meta_rel="${tbl}metadata/"
    ensure_managed_folder "${meta_rel}"
    apply_metadata_policy "${meta_rel}"

    # 2. 预建今明两天
    for day in 0 1; do
      day_str=$(date -u -d "+${day} days" +"${FOLDER_DATE_FORMAT}")
      day_rel="${tbl}data/${day_str}/"
      ensure_managed_folder "${day_rel}"
      apply_policy "${day_rel}" "${HOT_SA}" "${HOT_ROLE}" "HOT"
    done

    # 3. 毫秒级遍历该表 data/ 下所有历史分区
    for part in $(list_sub_folders "${tbl}data/"); do
      part_name=$(basename "${part}")
      epoch=$(parse_folder_epoch "${part_name}")
      [[ -z "${epoch}" ]] && continue

      age_days=$(( (TODAY_EPOCH - epoch) / 86400 ))
      ensure_managed_folder "${part}"

      if (( age_days < HOT_DAYS )); then
        apply_policy "${part}" "${HOT_SA}" "${HOT_ROLE}" "HOT  <60d"
      else
        apply_policy "${part}" "${COLD_SA}" "${COLD_ROLE}" "COLD >=60d"
      fi
    done
  done
done

echo "==> 权限调和全部完成！✔"
