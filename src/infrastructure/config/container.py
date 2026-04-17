"""Dependency Injection Container."""

from src.infrastructure.config.database import get_db_session
from src.infrastructure.adapter.persistence.repositories import (
    SQLAlchemyAccountRepository,
    SQLAlchemyTransactionRepository,
    SQLAlchemyLedgerEntryRepository,
)
from src.application.service.send_money_service import SendMoneyService


class Container:
    """Simple dependency injection container."""
    
    @staticmethod
    def get_send_money_service() -> SendMoneyService:
        """Get SendMoneyService with all dependencies.
        
        Returns:
            Configured SendMoneyService
        """
        db_session = get_db_session()
        
        account_repo = SQLAlchemyAccountRepository(db_session)
        transaction_repo = SQLAlchemyTransactionRepository(db_session)
        ledger_repo = SQLAlchemyLedgerEntryRepository(db_session)
        
        return SendMoneyService(account_repo, transaction_repo, ledger_repo)
