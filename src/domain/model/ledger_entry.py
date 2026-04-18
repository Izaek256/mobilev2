"""Ledger Entry domain model for double-entry bookkeeping."""

from uuid import UUID
from datetime import datetime
from typing import Optional
from src.domain.model.money import Money
from src.domain.model.enums import EntryType


class LedgerEntry:
    """Ledger entry for double-entry bookkeeping (DEBIT/CREDIT)."""
    
    def __init__(
        self,
        entry_id: UUID,
        transaction_id: UUID,
        account_id: str,
        entry_type: EntryType,
        amount: Money,
        created_at: Optional[datetime] = None,
    ):
        """Initialize a LedgerEntry.
        
        Args:
            entry_id: Unique entry identifier
            transaction_id: Associated transaction ID
            account_id: Associated account ID
            entry_type: DEBIT or CREDIT
            amount: Entry amount
            created_at: Creation timestamp
        """
        self.entry_id = entry_id
        self.transaction_id = transaction_id
        self.account_id = account_id
        self.entry_type = entry_type
        self.amount = amount
        self.created_at = created_at or datetime.utcnow()
    
    def is_debit(self) -> bool:
        """Check if entry is a debit."""
        return self.entry_type == EntryType.DEBIT
    
    def is_credit(self) -> bool:
        """Check if entry is a credit."""
        return self.entry_type == EntryType.CREDIT
    
    def __repr__(self) -> str:
        """Debug representation."""
        return (
            f"LedgerEntry(id={self.entry_id}, transaction={self.transaction_id}, "
            f"account={self.account_id}, type={self.entry_type}, amount={self.amount})"
        )
