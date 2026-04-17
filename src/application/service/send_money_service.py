"""Send Money Service - Implementation of SendMoneyUseCase."""

from uuid import uuid4
from src.domain.model.money import Money
from src.domain.model.transaction import Transaction
from src.domain.model.enums import TransactionType, TransactionStatus
from src.domain.port.in.send_money_command import SendMoneyCommand
from src.domain.port.in.send_money_use_case import SendMoneyUseCase
from src.domain.port.out.repositories import (
    AccountRepository,
    TransactionRepository,
    LedgerEntryRepository,
)
from src.domain.exception.domain_exception import OptimisticLockException
from src.application.service.transfer_executor import TransferExecutor


class SendMoneyService(SendMoneyUseCase):
    """Service implementing the SendMoneyUseCase.
    
    Handles idempotency, optimistic locking, and coordinates the transfer execution.
    """
    
    def __init__(
        self,
        account_repo: AccountRepository,
        transaction_repo: TransactionRepository,
        ledger_repo: LedgerEntryRepository,
    ):
        """Initialize the SendMoneyService.
        
        Args:
            account_repo: Account repository
            transaction_repo: Transaction repository
            ledger_repo: LedgerEntry repository
        """
        self.account_repo = account_repo
        self.transaction_repo = transaction_repo
        self.ledger_repo = ledger_repo
        self.transfer_executor = TransferExecutor(
            account_repo, transaction_repo, ledger_repo
        )
    
    def execute(self, command: SendMoneyCommand) -> None:
        """Execute the send money use case with idempotency and optimistic locking.
        
        The algorithm:
        1. Check if transaction already exists (idempotency)
        2. If exists and completed, return success (idempotent)
        3. If exists and pending, return (prevents duplicate processing)
        4. Otherwise, create new transaction and execute transfer
        5. Handle optimistic lock conflicts with retry logic
        
        Args:
            command: SendMoneyCommand with transfer details
            
        Raises:
            Various domain exceptions on failure
        """
        # Check for existing transaction (idempotency)
        existing_transaction = self.transaction_repo.find_by_idempotency_key(
            command.idempotency_key
        )
        
        if existing_transaction:
            # Idempotent: if already completed, return success
            if existing_transaction.is_completed():
                return
            # If still pending, prevent duplicate processing
            if existing_transaction.is_pending():
                return
        
        # Create new transaction
        transaction = Transaction(
            transaction_id=uuid4(),
            idempotency_key=command.idempotency_key,
            from_account_id=command.from_account_id,
            to_account_id=command.to_account_id,
            amount=Money(command.amount),
            transaction_type=TransactionType.TRANSFER,
            status=TransactionStatus.PENDING,
        )
        
        # Save the pending transaction
        self.transaction_repo.save(transaction)
        
        # Execute the transfer (handles optimistic locking internally)
        try:
            self.transfer_executor.execute_transfer(
                transaction,
                Money(command.amount),
            )
        except Exception as e:
            # Mark transaction as failed
            transaction.mark_failed()
            self.transaction_repo.save(transaction)
            raise
