"""SQLAlchemy ORM models mapped to database schema."""

from uuid import uuid4
from decimal import Decimal
from datetime import datetime
from sqlalchemy import (
    Column,
    String,
    Numeric,
    Integer,
    DateTime,
    ForeignKey,
    UniqueConstraint,
    Index,
    Boolean,
)
from sqlalchemy.types import TypeDecorator
import uuid as uuid_module

from src.infrastructure.config.database import Base


class GUID(TypeDecorator):
    """Platform-independent GUID type for SQLAlchemy.
    
    Uses CHAR(32) for SQLite and UUID for PostgreSQL.
    """
    impl = String(32)
    cache_ok = True
    
    def process_bind_param(self, value, dialect):
        if value is None:
            return value
        if isinstance(value, uuid_module.UUID):
            return str(value).replace("-", "")
        return value
    
    def process_result_value(self, value, dialect):
        if value is None:
            return value
        return uuid_module.UUID(value)


class AccountModel(Base):
    """SQLAlchemy model for Account."""
    
    __tablename__ = "accounts"
    
    account_id = Column(String(50), primary_key=True)
    owner_id = Column(String(255), nullable=False, index=True)
    balance = Column(Numeric(19, 2), nullable=False, default=0)
    currency = Column(String(3), nullable=False, default="USD")
    status = Column(String(50), nullable=False, default="ACTIVE")
    version = Column(Integer, nullable=False, default=0)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    __table_args__ = (
        Index("idx_accounts_owner_id", "owner_id"),
        Index("idx_accounts_status", "status"),
    )
    
    def __repr__(self) -> str:
        return f"<AccountModel(id={self.account_id}, owner={self.owner_id}, balance={self.balance})>"


class TransactionModel(Base):
    """SQLAlchemy model for Transaction."""
    
    __tablename__ = "transactions"
    
    transaction_id = Column(GUID, primary_key=True, default=uuid4)
    idempotency_key = Column(String(255), nullable=False, unique=True, index=True)
    from_account_id = Column(String(50), ForeignKey("accounts.account_id"), nullable=False)
    to_account_id = Column(String(50), ForeignKey("accounts.account_id"), nullable=False)
    amount = Column(Numeric(19, 2), nullable=False)
    currency = Column(String(3), nullable=False, default="USD")
    transaction_type = Column(String(50), nullable=False, default="TRANSFER")
    status = Column(String(50), nullable=False, default="PENDING", index=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    completed_at = Column(DateTime, nullable=True)
    
    __table_args__ = (
        Index("idx_transactions_from_account", "from_account_id"),
        Index("idx_transactions_to_account", "to_account_id"),
        Index("idx_transactions_status", "status"),
        Index("idx_transactions_idempotency", "idempotency_key"),
    )
    
    def __repr__(self) -> str:
        return (
            f"<TransactionModel(id={self.transaction_id}, key={self.idempotency_key}, "
            f"from={self.from_account_id}, to={self.to_account_id}, status={self.status})>"
        )


class LedgerEntryModel(Base):
    """SQLAlchemy model for LedgerEntry."""
    
    __tablename__ = "ledger_entries"
    
    entry_id = Column(GUID, primary_key=True, default=uuid4)
    transaction_id = Column(GUID, ForeignKey("transactions.transaction_id"), nullable=False)
    account_id = Column(String(50), ForeignKey("accounts.account_id"), nullable=False)
    entry_type = Column(String(10), nullable=False)  # DEBIT or CREDIT
    amount = Column(Numeric(19, 2), nullable=False)
    currency = Column(String(3), nullable=False, default="USD")
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    
    __table_args__ = (
        Index("idx_ledger_entries_transaction", "transaction_id"),
        Index("idx_ledger_entries_account", "account_id"),
    )
    
    def __repr__(self) -> str:
        return (
            f"<LedgerEntryModel(id={self.entry_id}, transaction={self.transaction_id}, "
            f"account={self.account_id}, type={self.entry_type}, amount={self.amount})>"
        )


class EventLogModel(Base):
    """SQLAlchemy model for Event Log (Event Sourcing)."""
    
    __tablename__ = "event_log"
    
    log_index = Column(Integer, primary_key=True, autoincrement=True)
    event_id = Column(String(50), nullable=False, unique=True, index=True)
    aggregate_id = Column(String(50), nullable=False, index=True) # E.g., account_id or transaction_id
    event_type = Column(String(100), nullable=False)
    payload = Column(String, nullable=False) # JSON payload
    timestamp = Column(DateTime, nullable=False, default=datetime.utcnow)
    
    __table_args__ = (
        Index("idx_event_log_aggregate", "aggregate_id"),
    )
    
    def __repr__(self) -> str:
        return f"<EventLogModel(index={self.log_index}, type={self.event_type}, aggregate={self.aggregate_id})>"


class RaftLogModel(Base):
    """SQLAlchemy model for Raft Log (Write-Ahead Log)."""
    
    __tablename__ = "raft_log"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    node_id = Column(String(50), nullable=False, index=True)
    term = Column(Integer, nullable=False)
    log_index = Column(Integer, nullable=False)
    entry_id = Column(String(100), nullable=False, unique=True)
    payload = Column(String, nullable=False)  # JSON payload
    applied = Column(Integer, nullable=False, default=0)  # Boolean (0/1)
    timestamp = Column(DateTime, nullable=False, default=datetime.utcnow)
    applied_at = Column(DateTime, nullable=True)
    
    __table_args__ = (
        UniqueConstraint("node_id", "log_index", name="uq_raft_node_index"),
        Index("idx_raft_log_node", "node_id"),
        Index("idx_raft_log_term", "term"),
        Index("idx_raft_log_applied", "applied"),
    )
    
    def __repr__(self) -> str:
        return f"<RaftLogModel(node={self.node_id}, term={self.term}, index={self.log_index}, applied={self.applied})>"


class ClusterStateModel(Base):
    """SQLAlchemy model for Cluster State (peer metadata)."""
    
    __tablename__ = "cluster_state"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    node_id = Column(String(50), nullable=False, unique=True, index=True)
    address = Column(String(255), nullable=False)  # e.g., "http://node-2:8000"
    role = Column(String(20), nullable=False, default="follower")  # leader, follower, candidate
    term = Column(Integer, nullable=False, default=0)
    last_heartbeat = Column(DateTime, nullable=True)
    status = Column(String(20), nullable=False, default="unknown")  # healthy, unhealthy, unknown
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    __table_args__ = (
        Index("idx_cluster_state_role", "role"),
        Index("idx_cluster_state_status", "status"),
    )
    
    def __repr__(self) -> str:
        return f"<ClusterStateModel(node={self.node_id}, role={self.role}, status={self.status})>"


class IdempotencyKeyModel(Base):
    """SQLAlchemy model for tracking idempotency keys across the cluster."""
    
    __tablename__ = "idempotency_keys"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    idempotency_key = Column(String(255), nullable=False, unique=True, index=True)
    transaction_id = Column(String(100), nullable=False)
    status = Column(String(20), nullable=False)  # pending, completed, failed
    result = Column(String, nullable=True)  # JSON result
    node_id = Column(String(50), nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    completed_at = Column(DateTime, nullable=True)
    
    __table_args__ = (
        Index("idx_idempotency_keys_transaction", "transaction_id"),
        Index("idx_idempotency_keys_status", "status"),
    )
    
    def __repr__(self) -> str:
        return f"<IdempotencyKeyModel(key={self.idempotency_key}, status={self.status})>"

