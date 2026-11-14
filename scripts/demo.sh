#!/bin/sh
# demo.sh —— 90 秒内可以录完的固定演示序列。
#
# 场景：
#   1. 干净状态下采集全部站点，记录当前行数与历史版本数
#   2. 上游改价（业务时间前进），再采集一轮
#   3. 观察：当前行数不变，历史版本增加，重复率保持 0
#   4. 任意时点还原：as-of 查询能取到改价前后的价格
#   5. 注入「业务时间倒退」，观察 items_stale 与被拒绝的观测
#   6. 注入 429，采集失败，观察游标状态
#
# 观察点：
#   - 第 3 步之后 rows 与 distinct_source 相等，versions 增长
#   - 第 4 步 as-of 在两个时点返回不同价格，revision 不同
#   - 第 5 步 items_written=0 / items_stale>0
set -eu

API="${API:-http://localhost:8081}"
MOCK="${MOCK:-http://localhost:9100}"

say() { printf '\n=== %s ===\n' "$1"; }

stats() {
  curl -fsS "$API/stats" | python3 -c '
import json,sys
d=json.load(sys.stdin)
print("total_rows", d["total_rows"])
for s in d["sites"]:
    print(" ", s["site_id"], "rows", s["rows"],
          "distinct", s["distinct_source_ids"],
          "versions", s.get("versions"),
          "dup", s.get("duplicate_ratio"))
'
}

say "0. 重置上游数据集"
curl -fsS -X POST "$MOCK/admin/reset" >/dev/null
curl -fsS -X POST "$MOCK/admin/fault/clear" >/dev/null

say "1. 干净状态采集全部站点"
for s in supplier catalog stock notice; do
  curl -fsS -X POST "$API/collect/$s" | head -c 200; echo
done
say "1.1 当前统计"
stats

say "2. 上游改价 30%（业务时间单调前进）"
curl -fsS -X POST "$MOCK/admin/shift?ratio=0.3"; echo

say "2.1 重新采集 supplier / catalog（全量复核，忽略游标）"
# cursor 模式的成功一轮会把游标推到末尾；演示里用 ?full=1 从第一页重扫，
# 以便观察到全部改动。生产里这是可选的运维复核，不是建立历史的前提。
for s in supplier catalog; do
  curl -fsS -X POST "$API/collect/$s?full=1" | head -c 200; echo
done

say "2.2 当前统计（rows 不应增长，versions 应增长）"
stats

say "3. 任意时点还原"
python3 - "$API" "$MOCK" <<'PY'
import json, sys, urllib.request, urllib.parse
api, mock = sys.argv[1], sys.argv[2]
def get(u): return json.load(urllib.request.urlopen(u))
state = get(mock + "/admin/state")
print("upstream versions:", {k: v["version"] for k, v in state.items()})
# 找一个有过改价的来源对象
items = get(mock + "/supplier/items?page_size=500")["items"]
changed = [i for i in items if i["updated_at"] >= "2026-11-03"]
if not changed:
    print("(本轮没有捕获到改价对象，跳过时点还原演示)")
    sys.exit(0)
sid = changed[0]["source_id"]
h = get(f"{api}/items/supplier/{sid}/history")
print(f"{sid} 版本链:")
for v in h["versions"]:
    print("  rev", v["revision"], "price", v["price_cents"],
          "valid_from", v["valid_from"], "valid_to", v.get("valid_to"))
old = h["versions"][-1]["valid_from"]
new = h["versions"][0]["valid_from"]
for at in (old, new):
    r = get(f"{api}/items/supplier/{sid}/as-of?at=" + urllib.parse.quote(at))
    v = r["version"]
    print(" as-of", at, "-> price", v["price_cents"], "rev", v["revision"])
PY

say "4. 注入业务时间倒退"
say "4.0 上游把 30% 条目的业务时间改成更早的值"
curl -fsS -X POST "$MOCK/admin/shift?ratio=0.3&time=back" >/dev/null; echo
curl -fsS -X POST "$API/collect/supplier?full=1" | python3 -c '
import json,sys
d=json.load(sys.stdin)
print("supplier:", d["status"], "wrote", d["items_written"],
      "stale", d.get("items_stale"), "unchanged", d.get("items_unchanged"))
'

say "5. 注入 40% 的 429，观察失败路径"
curl -fsS -X POST "$MOCK/admin/fault?site=supplier&kind=429&rate=0.4&seed=7"; echo
echo "（预期返回 502；失败轮次不推进游标）"
curl -sS -X POST "$API/collect/supplier?full=1" | head -c 300; echo
say "5.1 游标状态（失败轮次不推进游标）"
curl -fsS "$API/stats" | python3 -c "import json,sys; d=json.load(sys.stdin); print(json.dumps(d['cursors'], ensure_ascii=False, indent=2))"

say "6. 清理故障并查看运行记录"
curl -fsS -X POST "$MOCK/admin/fault/clear" >/dev/null
curl -fsS "$API/runs?limit=6" | python3 -c '
import json,sys
for r in json.load(sys.stdin)["runs"]:
    print(" ", r["site_id"], r["status"],
          "seen", r["items_seen"], "wrote", r["items_written"],
          "stale", r.get("items_stale"), "unchanged", r.get("items_unchanged"))
'
echo "done"
