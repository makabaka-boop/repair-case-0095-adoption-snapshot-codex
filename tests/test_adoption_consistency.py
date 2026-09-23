"""采用快照一致性验收（在真实 PostgreSQL 上覆盖并发场景）：

- 计算后修订方案再采用：快照中的方案、修订、切断清单、总费用必须
  全部来自计算记录本身，组成同一份不可混合的快照；
- 采用结果被替换后，旧计算不可再次采用（每个成功计算至多采用一次），
  历史采用次数保持为 1；
- 同一方案首次并发采用两个不同成功计算：不得误报冲突或返回 500，
  成功响应与最终查询到的采用结果一致。

前两类用例在任意数据库上运行；并发用例依赖行级锁，仅在真实
PostgreSQL（TEST_DATABASE_URL 指向 PostgreSQL）下执行。
"""

import threading
from collections import deque

import pytest

from app.db import engine

PLAN_V1 = {
    "zones": ["SRC1", "SRC2", "MID", "SAFE1", "SAFE2"],
    "segments": [
        {"id": "p1", "from": "SRC1", "to": "MID", "cost": 4},
        {"id": "p2", "from": "SRC2", "to": "MID", "cost": 6},
        {"id": "p3", "from": "MID", "to": "SAFE1", "cost": 5},
        {"id": "p4", "from": "MID", "to": "SAFE2", "cost": 7},
    ],
    "sources": ["SRC1", "SRC2"],
    "protections": ["SAFE1", "SAFE2"],
}
# 修订版：p1 费用 4 -> 100，最小割变为 p3+p4=12（源侧含 MID）
PLAN_V2 = dict(
    PLAN_V1,
    segments=[
        {"id": "p1", "from": "SRC1", "to": "MID", "cost": 100},
        {"id": "p2", "from": "SRC2", "to": "MID", "cost": 6},
        {"id": "p3", "from": "MID", "to": "SAFE1", "cost": 5},
        {"id": "p4", "from": "MID", "to": "SAFE2", "cost": 7},
    ],
)
RESULT_V1 = {
    "source_zones": ["SRC1", "SRC2"],
    "cut_segments": ["p1", "p2"],
    "total_cost": 10,
}
RESULT_V2 = {
    "source_zones": ["MID", "SRC1", "SRC2"],
    "cut_segments": ["p3", "p4"],
    "total_cost": 12,
}

requires_postgresql = pytest.mark.skipif(
    engine.url.get_backend_name() != "postgresql",
    reason="并发采用验收需要真实 PostgreSQL 行级锁（设置 TEST_DATABASE_URL）",
)


def put_plan(client, plan_id, plan):
    return client.put(f"/plans/{plan_id}", json=plan)


def compute(client, plan_id):
    return client.post(f"/plans/{plan_id}/computations")


def adopt(client, plan_id, computation_id):
    return client.post(
        f"/plans/{plan_id}/adopt", json={"computation_id": computation_id}
    )


def get_computation(client, plan_id, computation_id):
    return client.get(f"/plans/{plan_id}/computations/{computation_id}")


def assert_cut_isolates_snapshot(snapshot):
    """快照中的切断清单确实隔断快照方案的全部污染路径，且费用一致。"""
    plan, result = snapshot["plan"], snapshot["result"]
    cut = set(result["cut_segments"])
    adjacency = {}
    for seg in plan["segments"]:
        if seg["id"] not in cut:
            adjacency.setdefault(seg["from"], []).append(seg["to"])
    reachable = set()
    queue = deque(plan["sources"])
    while queue:
        zone = queue.popleft()
        if zone in reachable:
            continue
        reachable.add(zone)
        queue.extend(adjacency.get(zone, []))
    assert reachable.isdisjoint(plan["protections"])
    cost_by_id = {seg["id"]: seg["cost"] for seg in plan["segments"]}
    assert sum(cost_by_id[sid] for sid in cut) == result["total_cost"]


# ---------- 计算后修订再采用：快照不可混合 ----------

def test_adopt_after_plan_revision_keeps_source_snapshot(client):
    assert put_plan(client, "rev", PLAN_V1).status_code == 200
    computation = compute(client, "rev").json()
    assert computation["plan_revision"] == 1
    assert computation["plan"] == PLAN_V1  # 计算记录携带来源方案
    assert computation["result"] == RESULT_V1
    assert computation["adoption_count"] == 0

    # 设施工程师修订方案（rev 2），随后采用此前（rev 1）的计算结果
    assert put_plan(client, "rev", PLAN_V2).json()["revision"] == 2

    resp = adopt(client, "rev", computation["computation_id"])
    assert resp.status_code == 200
    snapshot = resp.json()
    # 快照完全来自计算记录：来源方案、来源修订、最小割结果同属一个版本
    assert snapshot["plan_id"] == "rev"
    assert snapshot["plan_revision"] == 1
    assert snapshot["plan"] == PLAN_V1
    assert snapshot["result"] == RESULT_V1
    assert snapshot["computation_id"] == computation["computation_id"]
    # 切断清单确实隔断快照中的污染路径，费用一致
    assert_cut_isolates_snapshot(snapshot)

    # 查询到的采用结果与采用响应完全一致
    assert client.get("/plans/rev/adoption").json() == snapshot
    # 采用不改写当前方案（仍为修订后的 rev 2）
    current = client.get("/plans/rev").json()
    assert current["revision"] == 2
    assert current["plan"] == PLAN_V2
    # 历史采用次数
    comp_after = get_computation(client, "rev", computation["computation_id"]).json()
    assert comp_after["adoption_count"] == 1


def test_recompute_after_revision_then_adopt_replaces_snapshot(client):
    put_plan(client, "rev2", PLAN_V1)
    old = compute(client, "rev2").json()["computation_id"]
    put_plan(client, "rev2", PLAN_V2)
    assert adopt(client, "rev2", old).status_code == 200

    # 对修订后的方案重新计算并采用，快照整体切换到新版本
    new = compute(client, "rev2").json()
    assert new["plan_revision"] == 2
    assert new["result"] == RESULT_V2
    resp = adopt(client, "rev2", new["computation_id"])
    assert resp.status_code == 200
    snapshot = resp.json()
    assert snapshot["plan_revision"] == 2
    assert snapshot["plan"] == PLAN_V2
    assert snapshot["result"] == RESULT_V2
    assert_cut_isolates_snapshot(snapshot)
    assert client.get("/plans/rev2/adoption").json() == snapshot


# ---------- 替换后重用旧计算：每个成功计算至多采用一次 ----------

def test_replaced_computation_cannot_be_adopted_again(client):
    put_plan(client, "reuse", PLAN_V1)
    comp_a = compute(client, "reuse").json()["computation_id"]
    comp_b = compute(client, "reuse").json()["computation_id"]

    assert adopt(client, "reuse", comp_a).status_code == 200
    # 另一计算替换当前采用结果
    assert adopt(client, "reuse", comp_b).status_code == 200
    current = client.get("/plans/reuse/adoption").json()
    assert current["computation_id"] == comp_b

    # 被替换的旧计算仍不可再次采用
    resp = adopt(client, "reuse", comp_a)
    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "COMPUTATION_ALREADY_ADOPTED"

    # 冲突不改变当前方案及已采用快照
    assert client.get("/plans/reuse/adoption").json() == current
    plan = client.get("/plans/reuse").json()
    assert plan["revision"] == 1
    assert plan["plan"] == PLAN_V1

    # 历史采用次数各为 1，失败的重复采用不增加计数
    for cid in (comp_a, comp_b):
        comp = get_computation(client, "reuse", cid).json()
        assert comp["adoption_count"] == 1


# ---------- 并发采用（真实 PostgreSQL） ----------

def _adopt_concurrently(plan_id, computation_ids):
    """多线程并发采用，barrier 对齐发起时机；返回 {tag: response}。"""
    from fastapi.testclient import TestClient

    from app.main import app

    barrier = threading.Barrier(len(computation_ids))
    responses = {}

    def worker(tag, cid):
        thread_client = TestClient(app)
        barrier.wait(timeout=10)
        responses[tag] = thread_client.post(
            f"/plans/{plan_id}/adopt", json={"computation_id": cid}
        )

    threads = [
        threading.Thread(target=worker, args=(f"t{i}", cid))
        for i, cid in enumerate(computation_ids)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)
    assert all(not thread.is_alive() for thread in threads), "adopt request hung"
    return responses


@requires_postgresql
def test_concurrent_first_adoption_of_two_computations(client):
    for round_no in range(5):
        plan_id = f"conc-{round_no}"
        put_plan(client, plan_id, PLAN_V1)
        comp_1 = compute(client, plan_id).json()["computation_id"]
        comp_2 = compute(client, plan_id).json()["computation_id"]

        responses = _adopt_concurrently(plan_id, [comp_1, comp_2])

        # 两个不同计算的并发首次采用：都成功，不得误报冲突或返回 500
        assert sorted(r.status_code for r in responses.values()) == [200, 200]
        snapshots = [r.json() for r in responses.values()]
        assert {s["computation_id"] for s in snapshots} == {comp_1, comp_2}
        # 每个成功响应都是内部一致的完整快照
        for snapshot in snapshots:
            assert snapshot["plan_id"] == plan_id
            assert snapshot["plan_revision"] == 1
            assert snapshot["plan"] == PLAN_V1
            assert snapshot["result"] == RESULT_V1
            assert_cut_isolates_snapshot(snapshot)
        # 最终查询到的采用结果与其中一个成功响应完全一致
        final = client.get(f"/plans/{plan_id}/adoption").json()
        assert final == snapshots[0] or final == snapshots[1]
        # 两个计算的历史采用次数各为 1
        for cid in (comp_1, comp_2):
            comp = get_computation(client, plan_id, cid).json()
            assert comp["adoption_count"] == 1


@requires_postgresql
def test_concurrent_adoption_of_same_computation_succeeds_once(client):
    for round_no in range(5):
        plan_id = f"conc-same-{round_no}"
        put_plan(client, plan_id, PLAN_V1)
        comp_id = compute(client, plan_id).json()["computation_id"]

        responses = _adopt_concurrently(plan_id, [comp_id, comp_id])

        # 同一计算并发采用：恰好一次成功，另一次确定地报已采用
        statuses = sorted(r.status_code for r in responses.values())
        assert statuses == [200, 409]
        by_status = {r.status_code: r for r in responses.values()}
        assert by_status[409].json()["error"]["code"] == "COMPUTATION_ALREADY_ADOPTED"
        # 最终记录与成功响应完全一致
        assert client.get(f"/plans/{plan_id}/adoption").json() == by_status[200].json()
        # 历史采用次数恰好为 1
        comp = get_computation(client, plan_id, comp_id).json()
        assert comp["adoption_count"] == 1
