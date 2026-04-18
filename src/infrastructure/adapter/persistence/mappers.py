"""Mappers between domain models and SQLAlchemy ORM models."""

from decimal import Decimal
from src.domain.model.account import Account
from src.domain.model.transaction import Transaction
from src.domain.model.ledger_entry import LedgerEntry
from src.domain.model.money import Money
from src.domain.model.enums import AccountStatus, TransactionType, TransactionStatus, EntryType
from src.infrastructure.adapter.persistence.sqlalchemy_models import (
    AccountModel,
    TransactionModel,
    LedgerEntryModel,
)


class AccountMapper:
    """Mapper between Account domain model and AccountModel ORM."""
    
    @staticmethod
    def to_domain(model: AccountModel) -> Account:
        """Convert ORM model to domain model.
        
        Args:
            model: SQLAlchemy AccountModel
            
        Returns:
            Account domain model
        """
        balance = Money(Decimal(str(model.balance)), model.currency)
        return Account(
            account_id=model.account_id,
            owner_id=model.owner_id,
            balance=balance,
            status=AccountStatus(model.status),
            version=model.version,
            created_at=model.created_at,
            updated_at=model.updated_at,
        )
    
    @staticmethod
    def to_persistence(domain: Account) -> AccountModel:
        """Convert domain model to ORM model.
        
        Args:
            domain: Account domain model
            
        Returns:
            SQLAlchemy AccountModel
        """
        return AccountModel(
            account_id=domain.account_id,
            owner_id=domain.owner_id,
            balance=domain.balance.amount,
            currency=domain.balance.currency,
            status=domain.status.value,
            version=domain.version,
            created_at=domain.created_at,
            updated_at=domain.updated_at,
        )


class TransactionMapper:
    """Mapper between Transaction domain model and TransactionModel ORM."""
    
    @staticmethod
    def to_domain(model: TransactionModel) -> Transaction:
        """Convert ORM model to domain model.
        
        Args:
            model: SQLAlchemy TransactionModel
            
        Returns:
            Transaction domain model
        """
        amount = Money(Decimal(str(model.amount)), model.currency)
        return Transaction(
            transaction_id=model.transaction_id,
            idempotency_key=model.idempotency_key,
            from_account_id=model.from_account_id,
            to_account_id=model.to_account_id,
            amount=amount,
            transaction_type=TransactionType(model.transaction_type),
            status=TransactionStatus(model.status),
            created_at=model.created_at,
            completed_at=model.completed_at,
        )
    
    @staticmethod
    def to_persistence(domain: Transaction) -> TransactionModel:
        """Convert domain model to ORM model.
        
        Args:
            domain: Transaction domain model
            
        Returns:
            SQLAlchemy TransactionModel
        """
        return TransactionModel(
            transaction_id=domain.transaction_id,
            idempotency_key=domain.idempotency_key,
            from_account_id=domain.from_account_id,
            to_account_id=domain.to_account_id,
            amount=domain.amount.amount,
            currency=domain.amount.currency,
            transaction_type=domain.transaction_type.value,
            status=domain.status.value,
            created_at=domain.created_at,
            completed_at=domain.completed_at,
        )


class LedgerEntryMapper:
    """Mapper between LedgerEntry domain model and LedgerEntryModel ORM."""
    
    @staticmethod
    def to_domain(model: LedgerEntryModel) -> LedgerEntry:
        """Convert ORM model to domain model.
        
        Args:
            model: SQLAlchemy LedgerEntryModel
            
        Returns:
            LedgerEntry domain model
        """
        amount = Money(Decimal(str(model.amount)), model.currency)
        return LedgerEntry(
            entry_id=model.entry_id,
            transaction_id=model.transaction_id,
            account_id=model.account_id,
            entry_type=EntryType(model.entry_type),
            amount=amount,
            created_at=model.created_at,
        )
    
    @staticmethod
    def to_persistence(domain: LedgerEntry) -> LedgerEntryModel:
        """Convert domain model to ORM model.
        
        Args:
            domain: LedgerEntry domain model
            
        Returns:
            SQLAlchemy LedgerEntryModel
        """
        return LedgerEntryModel(
            entry_id=domain.entry_id,
            transaction_id=domain.transaction_id,
            account_id=domain.account_id,
            entry_type=domain.entry_type.value,
            amount=domain.amount.amount,
            currency=domain.amount.currency,
            created_at=domain.created_at,
        )
