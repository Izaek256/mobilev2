"""Repository implementations using SQLAlchemy."""

from uuid import UUID
from typing import Optional
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

from src.domain.model.account import Account
from src.domain.model.transaction import Transaction
from src.domain.model.ledger_entry import LedgerEntry
from src.domain.port.out.repositories import (
    AccountRepository,
    TransactionRepository,
    LedgerEntryRepository,
)
from src.infrastructure.adapter.persistence.sqlalchemy_models import (
    AccountModel,
    TransactionModel,
    LedgerEntryModel,
)
from src.infrastructure.adapter.persistence.mappers import (
    AccountMapper,
    TransactionMapper,
    LedgerEntryMapper,
)
from src.domain.exception.domain_exception import OptimisticLockException


class SQLAlchemyAccountRepository(AccountRepository):
    """Account repository implementation using SQLAlchemy."""
    
    def __init__(self, session: Session):
        """Initialize the repository with a database session.
        
        Args:
            session: SQLAlchemy Session
        """
        self.session = session
    
    def save(self, account: Account) -> None:
        """Save an account (insert or update).
        
        Args:
            account: Account to save
            
        Raises:
            OptimisticLockException: If version conflict on update
        """
        model = AccountMapper.to_persistence(account)
        
        # Try to find existing account
        existing = self.session.query(AccountModel).filter(
            AccountModel.account_id == account.account_id
        ).first()
        
        if existing:
            # Update existing account with version check
            if existing.version != account.version - 1:
                # Version conflict - optimistic lock failed
                raise OptimisticLockException(
                    f"Account {account.account_id} was modified by another transaction"
                )
            # Update fields
            existing.balance = model.balance
            existing.currency = model.currency
            existing.status = model.status
            existing.version = model.version
            existing.updated_at = model.updated_at
        else:
            # Insert new account
            self.session.add(model)
        
        self.session.flush()
    
    def find_by_id(self, account_id: UUID) -> Optional[Account]:
        """Find account by ID.
        
        Args:
            account_id: Account identifier
            
        Returns:
            Account if found, None otherwise
        """
        model = self.session.query(AccountModel).filter(
            AccountModel.account_id == account_id
        ).first()
        
        if model:
            return AccountMapper.to_domain(model)
        return None
    
    def find_all(self) -> list[Account]:
        """Find all accounts.
        
        Returns:
            List of all accounts
        """
        models = self.session.query(AccountModel).all()
        return [AccountMapper.to_domain(model) for model in models]


class SQLAlchemyTransactionRepository(TransactionRepository):
    """Transaction repository implementation using SQLAlchemy."""
    
    def __init__(self, session: Session):
        """Initialize the repository with a database session.
        
        Args:
            session: SQLAlchemy Session
        """
        self.session = session
    
    def save(self, transaction: Transaction) -> None:
        """Save a transaction (insert or update).
        
        Args:
            transaction: Transaction to save
        """
        model = TransactionMapper.to_persistence(transaction)
        
        # Try to find existing transaction
        existing = self.session.query(TransactionModel).filter(
            TransactionModel.transaction_id == transaction.transaction_id
        ).first()
        
        if existing:
            # Update existing transaction
            existing.status = model.status
            existing.completed_at = model.completed_at
        else:
            # Insert new transaction
            try:
                self.session.add(model)
            except IntegrityError as e:
                # Idempotency key might already exist
                self.session.rollback()
                raise
        
        self.session.flush()
    
    def find_by_id(self, transaction_id: UUID) -> Optional[Transaction]:
        """Find transaction by ID.
        
        Args:
            transaction_id: Transaction identifier
            
        Returns:
            Transaction if found, None otherwise
        """
        model = self.session.query(TransactionModel).filter(
            TransactionModel.transaction_id == transaction_id
        ).first()
        
        if model:
            return TransactionMapper.to_domain(model)
        return None
    
    def find_by_idempotency_key(self, idempotency_key: str) -> Optional[Transaction]:
        """Find transaction by idempotency key.
        
        Args:
            idempotency_key: Idempotency key
            
        Returns:
            Transaction if found, None otherwise
        """
        model = self.session.query(TransactionModel).filter(
            TransactionModel.idempotency_key == idempotency_key
        ).first()
        
        if model:
            return TransactionMapper.to_domain(model)
        return None


class SQLAlchemyLedgerEntryRepository(LedgerEntryRepository):
    """Ledger entry repository implementation using SQLAlchemy."""
    
    def __init__(self, session: Session):
        """Initialize the repository with a database session.
        
        Args:
            session: SQLAlchemy Session
        """
        self.session = session
    
    def save(self, entry: LedgerEntry) -> None:
        """Save a ledger entry.
        
        Args:
            entry: LedgerEntry to save
        """
        model = LedgerEntryMapper.to_persistence(entry)
        self.session.add(model)
        self.session.flush()
    
    def find_by_transaction_id(self, transaction_id: UUID) -> list[LedgerEntry]:
        """Find all ledger entries for a transaction.
        
        Args:
            transaction_id: Transaction identifier
            
        Returns:
            List of ledger entries for the transaction
        """
        models = self.session.query(LedgerEntryModel).filter(
            LedgerEntryModel.transaction_id == transaction_id
        ).all()
        
        return [LedgerEntryMapper.to_domain(model) for model in models]
