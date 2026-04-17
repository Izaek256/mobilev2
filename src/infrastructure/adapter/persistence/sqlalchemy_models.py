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
)
from sqlalchemy.dialects.postgresql import UUID
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
    
    account_id = Column(GUID, primary_key=True, default=uuid4)
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
    from_account_id = Column(GUID, ForeignKey("accounts.account_id"), nullable=False)
    to_account_id = Column(GUID, ForeignKey("accounts.account_id"), nullable=False)
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
    account_id = Column(GUID, ForeignKey("accounts.account_id"), nullable=False)
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
