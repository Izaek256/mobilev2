"""Outbound port interfaces for repositories."""

from abc import ABC, abstractmethod
from uuid import UUID
from typing import Optional
from src.domain.model.account import Account
from src.domain.model.transaction import Transaction
from src.domain.model.ledger_entry import LedgerEntry


class AccountRepository(ABC):
    """Repository port for Account persistence."""
    
    @abstractmethod
    def save(self, account: Account) -> None:
        """Save an account.
        
        Args:
            account: Account to save
        """
        pass
    
    @abstractmethod
    def find_by_id(self, account_id: str) -> Optional[Account]:
        """Find account by ID.
        
        Args:
            account_id: Account identifier
            
        Returns:
            Account if found, None otherwise
        """
        pass
    
    @abstractmethod
    def find_all(self) -> list[Account]:
        """Find all accounts.
        
        Returns:
            List of all accounts
        """
        pass


class TransactionRepository(ABC):
    """Repository port for Transaction persistence."""
    
    @abstractmethod
    def save(self, transaction: Transaction) -> None:
        """Save a transaction.
        
        Args:
            transaction: Transaction to save
        """
        pass
    
    @abstractmethod
    def find_by_id(self, transaction_id: UUID) -> Optional[Transaction]:
        """Find transaction by ID.
        
        Args:
            transaction_id: Transaction identifier
            
        Returns:
            Transaction if found, None otherwise
        """
        pass
    
    @abstractmethod
    def find_by_idempotency_key(self, idempotency_key: str) -> Optional[Transaction]:
        """Find transaction by idempotency key (for deduplication).
        
        Args:
            idempotency_key: Idempotency key
            
        Returns:
            Transaction if found, None otherwise
        """
        pass


class LedgerEntryRepository(ABC):
    """Repository port for LedgerEntry persistence."""
    
    @abstractmethod
    def save(self, entry: LedgerEntry) -> None:
        """Save a ledger entry.
        
        Args:
            entry: LedgerEntry to save
        """
        pass
    
    @abstractmethod
    def find_by_transaction_id(self, transaction_id: UUID) -> list[LedgerEntry]:
        """Find all ledger entries for a transaction.
        
        Args:
            transaction_id: Transaction identifier
            
        Returns:
            List of ledger entries for the transaction
        """
        pass
