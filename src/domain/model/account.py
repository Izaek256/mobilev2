"""Account domain model."""

from uuid import UUID, uuid4
from datetime import datetime
from typing import Optional
from decimal import Decimal
from src.domain.model.money import Money
from src.domain.model.enums import AccountStatus
from src.domain.exception.domain_exception import (
    InsufficientFundsException,
    AccountInactiveException,
    InvalidMoneyException,
)


class Account:
    """Account entity with deposit, withdraw, and transaction capabilities."""
    
    def __init__(
        self,
        account_id: str,
        owner_id: str,
        balance: Money,
        status: AccountStatus = AccountStatus.ACTIVE,
        version: int = 0,
        created_at: Optional[datetime] = None,
        updated_at: Optional[datetime] = None,
    ):
        """Initialize an Account.
        
        Args:
            account_id: Unique account identifier
            owner_id: Owner identifier
            balance: Current account balance (Money object)
            status: Account status
            version: Optimistic lock version
            created_at: Creation timestamp
            updated_at: Last update timestamp
        """
        self.account_id = account_id
        self.owner_id = owner_id
        self._balance = balance
        self.status = status
        self.version = version
        self.created_at = created_at or datetime.utcnow()
        self.updated_at = updated_at or datetime.utcnow()
    
    @property
    def balance(self) -> Money:
        """Get current balance."""
        return self._balance
    
    def can_transact(self) -> bool:
        """Check if account can perform transactions."""
        return self.status == AccountStatus.ACTIVE
    
    def deposit(self, amount: Money) -> None:
        """Deposit money into the account.
        
        Args:
            amount: Amount to deposit
            
        Raises:
            AccountInactiveException: If account is not active
            InvalidMoneyException: If amount is invalid
        """
        if not self.can_transact():
            raise AccountInactiveException(f"Account {self.account_id} is not active")
        
        if not amount.is_positive():
            raise InvalidMoneyException("Deposit amount must be positive")
        
        self._balance = self._balance.add(amount)
        self.version += 1
        self.updated_at = datetime.utcnow()
    
    def withdraw(self, amount: Money) -> None:
        """Withdraw money from the account.
        
        Args:
            amount: Amount to withdraw
            
        Raises:
            AccountInactiveException: If account is not active
            InvalidMoneyException: If amount is invalid or negative
            InsufficientFundsException: If balance is insufficient
        """
        if not self.can_transact():
            raise AccountInactiveException(f"Account {self.account_id} is not active")
        
        if not amount.is_positive():
            raise InvalidMoneyException("Withdrawal amount must be positive")
        
        if self._balance < amount:
            raise InsufficientFundsException(
                f"Insufficient funds. Required: {amount}, Available: {self._balance}"
            )
        
        self._balance = self._balance.subtract(amount)
        self.version += 1
        self.updated_at = datetime.utcnow()
    
    def __repr__(self) -> str:
        """Debug representation."""
        return (
            f"Account(id={self.account_id}, owner={self.owner_id}, "
            f"balance={self._balance}, status={self.status}, version={self.version})"
        )
