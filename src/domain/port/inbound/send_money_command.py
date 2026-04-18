"""Send Money Command - Input/Request model for the use case."""

from dataclasses import dataclass
from uuid import UUID
from decimal import Decimal


@dataclass
class SendMoneyCommand:
    """Command to send money from one account to another.
    
    Args:
        from_account_id: Source account identifier
        to_account_id: Destination account identifier
        amount: Amount to transfer
        idempotency_key: Unique key for idempotent operations
    """
    from_account_id: str
    to_account_id: str
    amount: Decimal
    idempotency_key: str
