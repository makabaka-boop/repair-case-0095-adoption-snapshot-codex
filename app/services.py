"""业务逻辑：方案保存、最小割计算、采用与已采用结果查询。

事务约定：
- 方案校验通过后才写库，非法整版不会改写当前方案；
- 计算失败仅落一条 FAILED 记录，不触碰方案与已采用结果；
- 计算记录持久化来源方案与来源修订，采用快照完全取自计算记录，
  与方案的后续修订无关，计算记录、来源方案、来源修订与最小割结果
  永远组成同一份不可混合的快照；
- 同一计算在整个生命周期内至多被采用一次（被其他结果替换后亦然），
  由 computations.adoption_count 判定；同一方案的并发采用通过方案
  行锁串行化，成功响应与最终落库记录一致，冲突时回滚并返回稳定
  错误码，当前方案与已采用结果保持不变。
"""

import uuid
from datetime import datetime, timezone

from sqlalchemy.exc import IntegrityError

from . import models
from .errors import ApiError
from .flow import solve_min_cut


def get_plan_or_404(db, plan_id):
    plan = db.get(models.Plan, plan_id)
    if plan is None:
        raise ApiError(404, "PLAN_NOT_FOUND", f"plan {plan_id!r} does not exist")
    return plan


def save_plan(db, plan_id, canonical_payload):
    """保存（新建或整版替换）方案；payload 必须先通过校验。"""
    plan = db.get(models.Plan, plan_id)
    if plan is None:
        plan = models.Plan(plan_id=plan_id, revision=1, payload=canonical_payload)
        db.add(plan)
    else:
        plan.revision += 1
        plan.payload = canonical_payload
    db.commit()
    db.refresh(plan)
    return plan


def compute(db, plan_id):
    """对当前方案执行最小割计算并持久化计算记录。

    计算记录携带来源方案快照与来源修订，之后的采用快照完全取自
    该记录，与方案的后续修订无关。
    """
    plan = get_plan_or_404(db, plan_id)
    computation_id = uuid.uuid4().hex
    try:
        result = solve_min_cut(plan.payload)
    except Exception as exc:  # 已校验输入不应失败；兜底记录失败
        computation = models.Computation(
            computation_id=computation_id,
            plan_id=plan.plan_id,
            plan_revision=plan.revision,
            plan_payload=plan.payload,
            status="FAILED",
            result=None,
            error={"code": "INTERNAL_ERROR", "message": str(exc)},
            adoption_count=0,
        )
        db.add(computation)
        db.commit()
        raise ApiError(500, "COMPUTATION_FAILED", "min-cut computation failed")
    computation = models.Computation(
        computation_id=computation_id,
        plan_id=plan.plan_id,
        plan_revision=plan.revision,
        plan_payload=plan.payload,
        status="SUCCESS",
        result=result,
        error=None,
        adoption_count=0,
    )
    db.add(computation)
    db.commit()
    db.refresh(computation)
    return computation


def get_computation_or_404(db, plan_id, computation_id):
    computation = db.get(models.Computation, computation_id)
    if computation is None or computation.plan_id != plan_id:
        raise ApiError(
            404,
            "COMPUTATION_NOT_FOUND",
            f"computation {computation_id!r} does not exist for plan {plan_id!r}",
        )
    return computation


def adopt(db, plan_id, computation_id):
    """采用一次成功计算，保存来源方案 + 结果的完整快照。

    快照完全取自计算记录（来源方案、来源修订、最小割结果），与方案
    之后的修订无关，三者永远属于同一版本；同一计算在整个生命周期内
    至多被采用一次，被其他结果替换后仍不可再次采用；新的采用会替换
    该方案当前的已采用结果。

    同一方案的并发采用通过方案行锁（SELECT ... FOR UPDATE）串行化：
    锁等待结束后，读提交隔离级别下的后续查询能看到已提交的最新状态，
    因此并发首次采用不会产生误报冲突或 500，成功响应与最终落库记录
    一致。
    """
    # 行级锁串行化同一方案的并发采用（对不存在的行不加锁，直接 404）
    plan = db.get(models.Plan, plan_id, with_for_update=True)
    if plan is None:
        raise ApiError(404, "PLAN_NOT_FOUND", f"plan {plan_id!r} does not exist")
    computation = get_computation_or_404(db, plan_id, computation_id)
    if computation.status != "SUCCESS":
        raise ApiError(
            409,
            "COMPUTATION_NOT_ADOPTABLE",
            f"computation {computation_id!r} did not succeed",
        )
    if computation.adoption_count > 0:
        raise ApiError(
            409,
            "COMPUTATION_ALREADY_ADOPTED",
            f"computation {computation_id!r} has already been adopted",
        )

    adopted_at = datetime.now(timezone.utc)
    snapshot = {
        "plan_id": computation.plan_id,
        "plan_revision": computation.plan_revision,
        "computation_id": computation_id,
        "adopted_at": adopted_at.isoformat(),
        "plan": computation.plan_payload,
        "result": computation.result,
    }
    adoption = db.get(models.Adoption, plan_id)
    if adoption is None:
        adoption = models.Adoption(
            plan_id=plan_id,
            computation_id=computation_id,
            snapshot=snapshot,
            adopted_at=adopted_at,
        )
        db.add(adoption)
    else:
        adoption.computation_id = computation_id
        adoption.snapshot = snapshot
        adoption.adopted_at = adopted_at
    computation.adoption_count += 1
    try:
        db.commit()
    except IntegrityError:
        # 行锁下不会到达；兜底保持稳定的错误契约
        db.rollback()
        raise ApiError(
            409,
            "COMPUTATION_ALREADY_ADOPTED",
            f"computation {computation_id!r} has already been adopted",
        )
    db.refresh(adoption)
    return adoption


def get_adoption(db, plan_id):
    adoption = db.get(models.Adoption, plan_id)
    if adoption is None:
        raise ApiError(
            404,
            "ADOPTION_NOT_FOUND",
            f"plan {plan_id!r} has no adopted result",
        )
    return adoption
