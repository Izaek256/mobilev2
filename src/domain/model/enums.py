from enum import Enum


class TransactionType(str, Enum):
    """Type of transaction."""
    TRANSFER = "TRANSFER"
    DEPOSIT = "DEPOSIT"
    WITHDRAWAL = "WITHDRAWAL"


class TransactionStatus(str, Enum):
    """Status of a transaction."""
    PENDING = "PENDING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class AccountStatus(str, Enum):
    """Status of an account."""
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"
    FROZEN = "FROZEN"


class EntryType(str, Enum):
    """Double-entry bookkeeping entry types."""
    DEBIT = "DEBIT"
    CREDIT = "CREDIT"
