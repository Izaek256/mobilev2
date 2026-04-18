"""Domain events for event sourcing."""

from dataclasses import dataclass, field
from datetime import datetime
from uuid import UUID, uuid4
from typing import Any, Dict


@dataclass
class DomainEvent:
    """Base class for all domain events."""
    event_id: str = field(default_factory=lambda: str(uuid4()))
    timestamp: datetime = field(default_factory=datetime.utcnow)
    version: int = 1
    
    @property
    def event_type(self) -> str:
        return self.__class__.__name__

@dataclass
class AccountCreatedEvent(DomainEvent):
    account_id: str = ""
    owner_id: str = ""
    currency: str = ""
    initial_balance: str = ""

@dataclass
class MoneyDepositedEvent(DomainEvent):
    account_id: str = ""
    amount: str = ""
    currency: str = ""

@dataclass
class MoneyWithdrawnEvent(DomainEvent):
    account_id: str = ""
    amount: str = ""
    currency: str = ""

@dataclass
class TransactionCreatedEvent(DomainEvent):
    transaction_id: str = ""
    idempotency_key: str = ""
    from_account_id: str = ""
    to_account_id: str = ""
    amount: str = ""
    currency: str = ""

@dataclass
class TransactionCompletedEvent(DomainEvent):
    transaction_id: str = ""

@dataclass
class TransactionFailedEvent(DomainEvent):
    transaction_id: str = ""
    reason: str = ""
