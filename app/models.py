"""持久化模型：方案、计算记录、采用快照。"""

from sqlalchemy import JSON, Column, DateTime, ForeignKey, Integer, String, func

from .db import Base


class Plan(Base):
    __tablename__ = "plans"

    plan_id = Column(String(64), primary_key=True)
    revision = Column(Integer, nullable=False)
    payload = Column(JSON, nullable=False)  # 校验后的规范化方案
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )


class Computation(Base):
    __tablename__ = "computations"

    computation_id = Column(String(32), primary_key=True)
    plan_id = Column(
        String(64), ForeignKey("plans.plan_id", ondelete="CASCADE"),
        nullable=False, index=True,
    )
    plan_revision = Column(Integer, nullable=False)
    status = Column(String(16), nullable=False)  # SUCCESS / FAILED
    result = Column(JSON, nullable=True)
    error = Column(JSON, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class Adoption(Base):
    __tablename__ = "adoptions"

    plan_id = Column(
        String(64), ForeignKey("plans.plan_id", ondelete="CASCADE"), primary_key=True
    )
    # 一次成功计算最多被采用一次
    computation_id = Column(String(32), nullable=False, unique=True)
    snapshot = Column(JSON, nullable=False)  # 方案 + 结果的完整快照
    adopted_at = Column(DateTime(timezone=True), nullable=False)
