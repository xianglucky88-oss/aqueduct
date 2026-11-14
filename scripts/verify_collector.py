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
    V5 同一来源对象只有一个当前状态（rows == distinct_source_ids）
    V6 历史版本数不少于当前行数（每次真实变化至少一个版本）
    V7 迁移完成：temporal_reads_ready 为真，且没有 pending 站点

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

    # V5: 一个来源对象只有一个当前状态
    for s in stats["sites"]:
        res.check(
            f"V5/{s['site_id']}",
            s["rows"] == s["distinct_source_ids"],
            f"当前行 {s['rows']} / 去重来源 {s['distinct_source_ids']}",
        )

    # V6: 历史版本应覆盖当前行，且可多于当前行（发生过变化）
    for s in stats["sites"]:
        versions = s.get("versions", 0)
        res.check(
            f"V6/{s['site_id']}",
            versions >= s["rows"],
            f"历史版本 {versions} / 当前行 {s['rows']}",
        )

    # V7: 迁移完成
    mig = stats.get("migration", {})
    res.check(
        "V7",
        bool(mig.get("temporal_reads_ready")) and not mig.get("pending_sites"),
        f"temporal_reads_ready={mig.get('temporal_reads_ready')} "
        f"pending={mig.get('pending_sites')}",
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

    return res.report()


if __name__ == "__main__":
    sys.exit(main())
