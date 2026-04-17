"""Transaction domain model."""

from uuid import UUID, uuid4
from datetime import datetime
from typing import Optional
from src.domain.model.money import Money
from src.domain.model.enums import TransactionType, TransactionStatus


class Transaction:
    """Transaction entity for tracking money transfers with idempotency."""
    
    def __init__(
        self,
        transaction_id: UUID,
        idempotency_key: str,
        from_account_id: UUID,
        to_account_id: UUID,
        amount: Money,
        transaction_type: TransactionType = TransactionType.TRANSFER,
        status: TransactionStatus = TransactionStatus.PENDING,
        created_at: Optional[datetime] = None,
        completed_at: Optional[datetime] = None,
    ):
        """Initialize a Transaction.
        
        Args:
            transaction_id: Unique transaction identifier
            idempotency_key: Unique idempotency key for deduplication
            from_account_id: Source account ID
            to_account_id: Destination account ID
            amount: Transfer amount
            transaction_type: Type of transaction
            status: Transaction status
            created_at: Creation timestamp
            completed_at: Completion timestamp
        """
        self.transaction_id = transaction_id
        self.idempotency_key = idempotency_key
        self.from_account_id = from_account_id
        self.to_account_id = to_account_id
        self.amount = amount
        self.transaction_type = transaction_type
        self.status = status
        self.created_at = created_at or datetime.utcnow()
        self.completed_at = completed_at
    
    def mark_completed(self) -> None:
        """Mark transaction as completed."""
        self.status = TransactionStatus.COMPLETED
        self.completed_at = datetime.utcnow()
    
    def mark_failed(self) -> None:
        """Mark transaction as failed."""
        self.status = TransactionStatus.FAILED
    
    def is_pending(self) -> bool:
        """Check if transaction is pending."""
        return self.status == TransactionStatus.PENDING
    
    def is_completed(self) -> bool:
        """Check if transaction is completed."""
        return self.status == TransactionStatus.COMPLETED
    
    def __repr__(self) -> str:
        """Debug representation."""
        return (
            f"Transaction(id={self.transaction_id}, key={self.idempotency_key}, "
            f"from={self.from_account_id}, to={self.to_account_id}, "
            f"amount={self.amount}, status={self.status})"
        )
