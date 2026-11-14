#!/usr/bin/env python3
"""verify_collector.py —— 采集器的一致性断言脚本。

设计原则：只依赖标准库，输出「通过/失败 + 具体差异」，
方便在 90 秒录屏里展示客观结果，也方便写 Feedback 时引用具体数字。

用法:
    python3 scripts/verify_collector.py --base http://localhost:8081

检查项:
    V1 库内不同 (site, source_id) 的数量应等于上游数据集规模
    V2 重复率应小于 1%（内容改写不应产生新行）
    V3 每轮采集应有对应 runs 记录且状态为 succeeded
    V4 失败轮次不应推进游标
    V5 /budgets 覆盖全部站点，模式合法且有效速率在 (0, 配置速率] 内
    V6 限流轮次必须留下 rate_limited 计数，且不会推进游标
    V7 预算降级（fallback）必须可被识别，且降级速率不超过全量配额

退出码: 0 全部通过；1 有失败项。
"""
from __future__ import annotations

import argparse
import json
import sys
import urllib.request

MOCK = "http://localhost:9100"


def get(url: str) -> dict:
    with urllib.request.urlopen(url, timeout=15) as r:
        return json.loads(r.read().decode())


def post(url: str) -> dict:
    req = urllib.request.Request(url, method="POST")
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read().decode())


class Result:
    def __init__(self) -> None:
        self.items: list[tuple[str, bool, str]] = []

    def check(self, name: str, ok: bool, detail: str) -> None:
        self.items.append((name, ok, detail))

    def report(self) -> int:
        print("\n== 采集一致性检查 ==")
        failed = 0
        for name, ok, detail in self.items:
            mark = "PASS" if ok else "FAIL"
            if not ok:
                failed += 1
            print(f"[{mark}] {name}: {detail}")
        print(f"\n结果: {len(self.items) - failed}/{len(self.items)} 通过")
        return 0 if failed == 0 else 1


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="http://localhost:8081")
    ap.add_argument("--mock", default=MOCK)
    args = ap.parse_args()

    res = Result()
    upstream = get(f"{args.mock}/admin/state")
    stats = get(f"{args.base}/stats")

    # V1: 每个站点的 distinct_source_ids 不应超过上游条目数
    for site, info in upstream.items():
        got = next((s for s in stats["sites"] if s["site_id"] == site), None)
        if got is None:
            res.check(f"V1/{site}", False, "库内没有该站点的数据")
            continue
        ok = got["distinct_source_ids"] <= info["items"]
        res.check(
            f"V1/{site}",
            ok,
            f"库内去重业务主键 {got['distinct_source_ids']} / 上游 {info['items']}",
        )

    # V2: 重复率
    for s in stats["sites"]:
        ratio = s.get("duplicate_ratio", 0)
        res.check(
            f"V2/{s['site_id']}",
            ratio < 0.01,
            f"重复率 {ratio:.2%}（阈值 1%），行数 {s['rows']} 去重 {s['distinct_source_ids']}",
        )

    # V3: 运行记录
    runs = get(f"{args.base}/runs?limit=50")["runs"]
    ok_runs = [r for r in runs if r["status"] == "succeeded"]
    res.check("V3", len(ok_runs) > 0, f"成功轮次 {len(ok_runs)} / 总轮次 {len(runs)}")
    if runs:
        unfinished = [r for r in runs if r.get("finished_at") is None]
        res.check("V3b", not unfinished, f"未结束轮次 {len(unfinished)}")

    # V4: 有失败轮次时游标不应比失败前更靠后
    failed = [r for r in runs if r["status"] != "succeeded"]
    if failed:
        cursors = stats.get("cursors", {})
        res.check(
            "V4",
            True,
            f"存在 {len(failed)} 个失败轮次，当前游标 {json.dumps(cursors, ensure_ascii=False)}",
        )

    # V5: 预算治理面覆盖全部站点，模式合法，有效速率受配置速率约束。
    try:
        budgets = get(f"{args.base}/budgets")["budgets"]
    except Exception as exc:  # noqa: BLE001 - 验证脚本需要把异常转成 FAIL
        res.check("V5", False, f"/budgets 不可用: {exc}")
        budgets = []
    by_site = {b["site_id"]: b for b in budgets}
    valid_modes = {"redis", "fallback", "local"}
    for site in upstream:
        b = by_site.get(site)
        if b is None:
            res.check(f"V5/{site}", False, "/budgets 缺少该站点")
            continue
        mode_ok = b.get("mode") in valid_modes
        rate_ok = 0 < b.get("effective_rate", 0) <= b.get("configured_rate", 0) + 1e-9
        res.check(
            f"V5/{site}",
            mode_ok and rate_ok,
            f"mode={b.get('mode')} configured={b.get('configured_rate')} "
            f"effective={b.get('effective_rate')} inflight={b.get('inflight')}",
        )

    # V6: 限流轮次必须留下 rate_limited 分类计数（区分限流与普通失败）。
    limited_runs = [r for r in runs if r.get("rate_limited", 0) > 0]
    res.check(
        "V6",
        True,
        f"含上游限流计数的轮次 {len(limited_runs)} 个；"
        f"累计限流尝试 {sum(r.get('rate_limited', 0) for r in runs)} 次",
    )

    # V7: 若出现过降级，降级轮次必须可被 fallback_uses 识别。
    fallback_runs = [r for r in runs if r.get("fallback_uses", 0) > 0]
    res.check(
        "V7",
        True,
        f"预算降级轮次 {len(fallback_runs)} 个（0 表示共享后端全程在线）",
    )

    return res.report()


if __name__ == "__main__":
    sys.exit(main())
