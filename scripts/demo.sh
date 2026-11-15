#!/bin/sh
# demo.sh —— 固定演示序列，覆盖去重、故障退避与终止/接管语义。
#
# 场景：
#   1. 干净状态采集，记录行数与游标
#   2. 上游改价（内容改写），再采集一轮：行数不应增长
#   3. 注入 429：本轮失败，站点按退避排期，游标停在检查点
#   4. 调度状态与探针
set -eu

API="${API:-http://localhost:8081}"
MOCK="${MOCK:-http://localhost:9100}"

say() { printf '\n=== %s ===\n' "$1"; }

say "0. 重置上游数据集"
curl -fsS -X POST "$MOCK/admin/reset" >/dev/null
curl -fsS -X POST "$MOCK/admin/fault/clear" >/dev/null

say "1. 干净状态采集 supplier / catalog"
curl -fsS -X POST "$API/collect/supplier" | head -c 400; echo
curl -fsS -X POST "$API/collect/catalog" | head -c 400; echo

say "1.1 当前统计"
curl -fsS "$API/stats" | python3 -m json.tool | head -40

say "2. 上游改价 30%"
curl -fsS -X POST "$MOCK/admin/shift?ratio=0.3"; echo

say "2.1 重新采集 supplier / catalog（内容原地更新）"
curl -fsS -X POST "$API/collect/supplier" | head -c 400; echo
curl -fsS -X POST "$API/collect/catalog" | head -c 400; echo

say "2.2 统计（total_rows 应与 1.1 相同）"
curl -fsS "$API/stats" | python3 -c "
import json,sys
d=json.load(sys.stdin)
print('total_rows =', d['total_rows'])
for s in d['sites']:
    print(f\"  {s['site_id']}: rows={s['rows']} distinct={s['distinct_source_ids']} dup={s['duplicate_ratio']:.2%}\")
"

say "3. 注入 40% 的 429"
curl -fsS -X POST "$MOCK/admin/fault?site=supplier&kind=429&rate=0.4&seed=7"; echo
curl -fsS -X POST "$API/collect/supplier" | head -c 300; echo

say "3.1 站点调度状态（next_run_at 应已推后，游标停在检查点）"
curl -fsS "$API/sites" | python3 -m json.tool | head -50

say "4. 探针"
for p in startupz livez readyz; do
  printf "  %s -> %s\n" "$p" "$(curl -s -o /dev/null -w '%{http_code}' "$API/$p")"
done

say "5. 清理故障"
curl -fsS -X POST "$MOCK/admin/fault/clear" >/dev/null
echo "done"
