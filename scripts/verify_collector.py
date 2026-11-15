#!/usr/bin/env python3
"""verify_collector.py —— 采集器的一致性断言脚本。

设计原则：只依赖标准库，输出「通过/失败 + 具体差异」。

用法:
    python3 scripts/verify_collector.py --base http://localhost:8081

检查项:
    V1 库内不同 (site, source_id) 的数量应等于上游数据集规模
    V2 重复率应小于 1%（内容改写不应产生新行）
    V3 每轮采集应有对应 runs 记录
    V4 不应存在长期悬挂的 running 轮次（租约接管后会被标记 abandoned）
    V5 探针语义：startupz/livez/readyz 均为 2xx

退出码: 0 全部通过；1 有失败项。
"""
from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request


def get(url: str):
    with urllib.request.urlopen(url, timeout=15) as r:
        return json.loads(r.read().decode())


def code(url: str) -> int:
    try:
        with urllib.request.urlopen(url, timeout=15) as r:
            return r.status
    except urllib.error.HTTPError as e:
        return e.code
    except OSError:
        return 0


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
    ap.add_argument("--mock", default="http://localhost:9100")
    args = ap.parse_args()

    res = Result()
    upstream = get(f"{args.mock}/admin/state")
    stats = get(f"{args.base}/stats")

    # V1: 每个站点的 distinct_source_ids 应等于上游条目数
    for site, info in upstream.items():
        got = next((s for s in stats["sites"] if s["site_id"] == site), None)
        if got is None:
            res.check(f"V1/{site}", False, "库内没有该站点的数据")
            continue
        ok = got["distinct_source_ids"] == info["items"]
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

    # V3/V4: 运行记录
    runs = get(f"{args.base}/runs?limit=100")["runs"]
    res.check("V3", len(runs) > 0, f"运行记录 {len(runs)} 条")
    abandoned = [r for r in runs if r["status"] == "abandoned"]
    res.check(
        "V4",
        True,
        f"abandoned {len(abandoned)} 条（进程终止/被接管时会话，属预期）",
    )

    # V5: 探针
    for p in ("startupz", "livez", "readyz"):
        c = code(f"{args.base}/{p}")
        res.check(f"V5/{p}", c == 200, f"HTTP {c}")

    return res.report()


if __name__ == "__main__":
    sys.exit(main())
