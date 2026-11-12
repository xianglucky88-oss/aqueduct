#!/bin/sh
# demo.sh —— 90 秒内可以录完的固定演示序列。
#
# 场景：
#   1. 干净状态下采集一轮，记录行数
#   2. 上游改价（内容改写），再采集一轮
#   3. 观察行数膨胀与重复率
#   4. 注入 429，采集失败，观察游标状态
#
# 观察点：
#   - 第 2 步之后 total_rows 与 distinct_source 之和的差值
#   - 第 4 步之后 cursors.supplier 的取值
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

say "2.1 重新采集 supplier / catalog"
curl -fsS -X POST "$API/collect/supplier" | head -c 400; echo
curl -fsS -X POST "$API/collect/catalog" | head -c 400; echo

say "2.2 当前统计"
curl -fsS "$API/stats" | python3 -m json.tool | head -40

say "3. 注入 40% 的 429"
curl -fsS -X POST "$MOCK/admin/fault?site=supplier&kind=429&rate=0.4&seed=7"; echo
curl -fsS -X POST "$API/collect/supplier" | head -c 300; echo

say "3.1 游标状态"
curl -fsS "$API/stats" | python3 -c "import json,sys; d=json.load(sys.stdin); print(json.dumps(d['cursors'], ensure_ascii=False, indent=2))"

say "4. 清理故障"
curl -fsS -X POST "$MOCK/admin/fault/clear" >/dev/null
echo "done"
