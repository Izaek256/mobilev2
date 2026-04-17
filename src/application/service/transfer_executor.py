"""Transfer executor - handles the core transfer logic within a transaction."""

from uuid import uuid4
from decimal import Decimal
from src.domain.model.money import Money
from src.domain.model.account import Account
from src.domain.model.transaction import Transaction
from src.domain.model.ledger_entry import LedgerEntry
from src.domain.model.enums import TransactionType, EntryType
from src.domain.port.out.repositories import (
    AccountRepository,
    TransactionRepository,
    LedgerEntryRepository,
)
from src.domain.exception.domain_exception import (
    AccountNotFoundException,
    InsufficientFundsException,
)


class TransferExecutor:
    """Executes the transfer logic within a database transaction."""
    
    def __init__(
        self,
        account_repo: AccountRepository,
        transaction_repo: TransactionRepository,
        ledger_repo: LedgerEntryRepository,
    ):
        """Initialize the transfer executor.
        
        Args:
            account_repo: Account repository
            transaction_repo: Transaction repository
            ledger_repo: LedgerEntry repository
        """
        self.account_repo = account_repo
        self.transaction_repo = transaction_repo
        self.ledger_repo = ledger_repo
    
    def execute_transfer(
        self,
        transaction: Transaction,
        amount: Money,
    ) -> None:
        """Execute a money transfer between two accounts.
        
        Atomically:
        1. Load both accounts with locks
        2. Check sufficient funds
        3. Update account balances
        4. Create ledger entries (double-entry bookkeeping)
        5. Mark transaction as completed
        
        Args:
            transaction: Transaction object to track the transfer
            amount: Amount to transfer
            
        Raises:
            AccountNotFoundException: If account not found
            InsufficientFundsException: If insufficient balance
        """
        # Load source and destination accounts
        from_account = self.account_repo.find_by_id(transaction.from_account_id)
        if not from_account:
            raise AccountNotFoundException(
                f"Source account {transaction.from_account_id} not found"
            )
        
        to_account = self.account_repo.find_by_id(transaction.to_account_id)
        if not to_account:
            raise AccountNotFoundException(
                f"Destination account {transaction.to_account_id} not found"
            )
        
        # Perform the transfer
        from_account.withdraw(amount)
        to_account.deposit(amount)
        
        # Save updated accounts
        self.account_repo.save(from_account)
        self.account_repo.save(to_account)
        
        # Create ledger entries (double-entry bookkeeping)
        debit_entry = LedgerEntry(
            entry_id=uuid4(),
            transaction_id=transaction.transaction_id,
            account_id=transaction.from_account_id,
            entry_type=EntryType.DEBIT,
            amount=amount,
        )
        
        credit_entry = LedgerEntry(
            entry_id=uuid4(),
            transaction_id=transaction.transaction_id,
            account_id=transaction.to_account_id,
            entry_type=EntryType.CREDIT,
            amount=amount,
        )
        
        self.ledger_repo.save(debit_entry)
        self.ledger_repo.save(credit_entry)
        
        # Mark transaction as completed
        transaction.mark_completed()
        self.transaction_repo.save(transaction)
