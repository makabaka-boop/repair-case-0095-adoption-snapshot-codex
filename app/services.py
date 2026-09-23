"""业务逻辑：方案保存、最小割计算、采用与已采用结果查询。

事务约定：
- 方案校验通过后才写库，非法整版不会改写当前方案；
- 计算失败仅落一条 FAILED 记录，不触碰方案与已采用结果；
- 采用通过 computation_id 唯一约束保证"一次成功计算只能采用一次"，
  冲突时回滚并返回稳定错误码，已采用结果保持不变。
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
    """对当前方案执行最小割计算并持久化计算记录。"""
    plan = get_plan_or_404(db, plan_id)
    computation_id = uuid.uuid4().hex
    try:
        result = solve_min_cut(plan.payload)
    except Exception as exc:  # 已校验输入不应失败；兜底记录失败
        computation = models.Computation(
            computation_id=computation_id,
            plan_id=plan.plan_id,
            plan_revision=plan.revision,
            status="FAILED",
            result=None,
            error={"code": "INTERNAL_ERROR", "message": str(exc)},
        )
        db.add(computation)
        db.commit()
        raise ApiError(500, "COMPUTATION_FAILED", "min-cut computation failed")
    computation = models.Computation(
        computation_id=computation_id,
        plan_id=plan.plan_id,
        plan_revision=plan.revision,
        status="SUCCESS",
        result=result,
        error=None,
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
    """采用一次成功计算，保存方案 + 结果的完整快照。

    同一计算只能被采用一次；新的采用会替换该方案当前的已采用结果。
    """
    plan = get_plan_or_404(db, plan_id)
    computation = get_computation_or_404(db, plan_id, computation_id)
    if computation.status != "SUCCESS":
        raise ApiError(
            409,
            "COMPUTATION_NOT_ADOPTABLE",
            f"computation {computation_id!r} did not succeed",
        )
    existing = (
        db.query(models.Adoption)
        .filter(models.Adoption.computation_id == computation_id)
        .first()
    )
    if existing is not None:
        raise ApiError(
            409,
            "COMPUTATION_ALREADY_ADOPTED",
            f"computation {computation_id!r} has already been adopted",
        )

    adopted_at = datetime.now(timezone.utc)
    snapshot = {
        "plan_id": plan.plan_id,
        "plan_revision": plan.revision,
        "computation_id": computation_id,
        "adopted_at": adopted_at.isoformat(),
        "plan": plan.payload,
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
    try:
        db.commit()
    except IntegrityError:
        # 并发下同一计算被重复采用
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
