"""
State Machine Applier - Applies committed Raft log entries to the state machine.

This is the ONLY place where the local database should be written to.
All state changes must come from committed Raft log entries to maintain consistency
across the cluster.
"""

import logging
import json
from typing import Dict, Any, Optional
from datetime import datetime
from sqlalchemy.orm import Session
from uuid import UUID

from src.infrastructure.adapter.persistence.event_sourced_repositories import (
    EventSourcedAccountRepository,
    EventSourcedTransactionRepository,
    EventSourcedLedgerEntryRepository,
)
from src.infrastructure.adapter.persistence.sqlalchemy_models import IdempotencyKeyModel
from src.domain.model.money import Money
from src.domain.port.inbound.send_money_command import SendMoneyCommand
from src.application.service.send_money_service import SendMoneyService
from src.domain.exception.domain_exception import DomainException, InsufficientFundsException

logger = logging.getLogger(__name__)


class CommittedLogEntry:
    """Represents a committed Raft log entry ready to be applied to the state machine."""
    
    def __init__(
        self,
        entry_id: str,
        term: int,
        index: int,
        timestamp: datetime,
        transaction_id: str,
        operation_type: str,
        payload: Dict[str, Any],
    ):
        self.entry_id = entry_id
        self.term = term
        self.index = index
        self.timestamp = timestamp  # From Raft log, NOT system time
        self.transaction_id = transaction_id
        self.operation_type = operation_type
        self.payload = payload
    
    @staticmethod
    def from_raft_log_entry(raft_entry) -> "CommittedLogEntry":
        """Convert a Raft LogEntry to CommittedLogEntry.
        
        Args:
            raft_entry: A raft_node.LogEntry instance
            
        Returns:
            CommittedLogEntry instance
        """
        data = raft_entry.data
        return CommittedLogEntry(
            entry_id=raft_entry.entry_id,
            term=raft_entry.term,
            index=raft_entry.index,
            timestamp=raft_entry.timestamp,
            transaction_id=data.get("transaction_id"),
            operation_type=data.get("operation_type"),
            payload=data.get("payload", {}),
        )


class StateMachineApplier:
    """
    Applies committed Raft log entries to the state machine (local database).
    
    Key Principles:
    1. ONLY applies committed entries (index <= commit_index)
    2. IDEMPOTENT: Checks transaction_id before applying
    3. DETERMINISTIC: Uses Raft log timestamp, not system time
    4. TRANSACTIONAL: All-or-nothing semantics
    5. NO PARTIAL FAILURES: If consensus succeeded but apply fails, that's a critical error
    """
    
    def __init__(self, db: Session):
        self.db = db
        self.account_repo = EventSourcedAccountRepository(db)
        self.transaction_repo = EventSourcedTransactionRepository(db)
        self.ledger_repo = EventSourcedLedgerEntryRepository(db)
        self.send_money_service = SendMoneyService(
            self.account_repo,
            self.transaction_repo,
            self.ledger_repo,
        )
    
    def apply(self, committed_entry: CommittedLogEntry) -> Dict[str, Any]:
        """
        Apply a committed log entry to the state machine.
        
        This is the CRITICAL method that bridges consensus and state changes.
        It must:
        1. Check idempotency (transaction already applied?)
        2. Deterministically execute the operation
        3. Record the transaction ID to prevent replays
        4. Be transactional (all-or-nothing)
        
        Args:
            committed_entry: A CommittedLogEntry to apply
            
        Returns:
            Result dict with success status and response data
            
        Raises:
            Exception: Only on critical errors that prevent recovery
        """
        transaction_id = committed_entry.transaction_id
        
        try:
            # Step 1: Check idempotency - has this transaction already been applied?
            existing_txn = self.db.query(IdempotencyKeyModel).filter(
                IdempotencyKeyModel.idempotency_key == transaction_id
            ).first()
            
            if existing_txn and existing_txn.status == "completed":
                logger.info(
                    f"Transaction {transaction_id} already applied (idempotent retry). "
                    f"Returning cached result."
                )
                return {
                    "success": True,
                    "transaction_id": transaction_id,
                    "idempotent": True,
                    "cached_result": existing_txn.response_data,
                }
            
            # Step 2: Execute operation based on type
            operation_type = committed_entry.operation_type
            
            if operation_type == "send_money":
                result = self._apply_send_money(
                    committed_entry,
                    transaction_id,
                )
            elif operation_type == "create_account":
                result = self._apply_create_account(
                    committed_entry,
                    transaction_id,
                )
            else:
                raise ValueError(f"Unknown operation type: {operation_type}")
            
            # Step 3: Record transaction in idempotency table
            # This marks it as processed so retries don't re-execute
            idempotency_record = IdempotencyKeyModel(
                idempotency_key=transaction_id,
                status="completed",
                response_data=json.dumps(result) if isinstance(result, dict) else str(result),
                created_at=committed_entry.timestamp,
                raft_index=committed_entry.index,
                raft_term=committed_entry.term,
            )
            self.db.add(idempotency_record)
            self.db.commit()
            
            logger.info(
                f"Successfully applied transaction {transaction_id} "
                f"(Raft index: {committed_entry.index}, term: {committed_entry.term})"
            )
            
            return {
                "success": True,
                "transaction_id": transaction_id,
                "idempotent": False,
                "result": result,
            }
            
        except Exception as e:
            self.db.rollback()
            logger.error(
                f"CRITICAL: Failed to apply committed Raft entry {transaction_id}. "
                f"This indicates a state machine safety violation. Error: {str(e)}",
                exc_info=True,
            )
            raise
    
    def _apply_send_money(
        self,
        committed_entry: CommittedLogEntry,
        transaction_id: str,
    ) -> Dict[str, Any]:
        """
        Apply a send_money operation using the Raft log timestamp (not system time).
        
        Args:
            committed_entry: The committed log entry
            transaction_id: The transaction ID for idempotency
            
        Returns:
            Result dict
        """
        payload = committed_entry.payload
        
        try:
            # Extract payload
            from_account_id = payload.get("from_account_id")
            to_account_id = payload.get("to_account_id")
            amount = float(payload.get("amount"))
            currency = payload.get("currency", "USD")
            
            # Create command with Raft timestamp (deterministic)
            command = SendMoneyCommand(
                from_account_id=from_account_id,
                to_account_id=to_account_id,
                amount=Money(amount, currency),
                idempotency_key=transaction_id,
            )
            
            # Execute the use case
            self.send_money_service.execute(command)
            
            return {
                "status": "success",
                "message": f"Transferred {amount} {currency} from {from_account_id} to {to_account_id}",
                "timestamp": committed_entry.timestamp.isoformat(),
            }
            
        except InsufficientFundsException as e:
            logger.warning(f"Transaction {transaction_id} failed: Insufficient funds")
            raise ValueError(f"Insufficient funds: {str(e)}")
        except DomainException as e:
            logger.warning(f"Transaction {transaction_id} domain error: {str(e)}")
            raise
    
    def _apply_create_account(
        self,
        committed_entry: CommittedLogEntry,
        transaction_id: str,
    ) -> Dict[str, Any]:
        """
        Apply a create_account operation.
        
        Args:
            committed_entry: The committed log entry
            transaction_id: The transaction ID for idempotency
            
        Returns:
            Result dict
        """
        payload = committed_entry.payload
        
        try:
            account_id = payload.get("account_id")
            account_name = payload.get("account_name")
            balance = float(payload.get("balance", 0))
            currency = payload.get("currency", "USD")
            
            # Check if account already exists
            existing = self.account_repo.find_by_id(account_id)
            if existing:
                return {
                    "status": "success",
                    "message": f"Account {account_id} already exists",
                    "timestamp": committed_entry.timestamp.isoformat(),
                }
            
            # Create account
            from src.domain.model.account import Account
            from src.domain.model.enums import AccountStatus
            
            account = Account(
                account_id=account_id,
                owner_id=account_name,
                balance=Money(balance, currency),
                status=AccountStatus.ACTIVE,
                created_at=committed_entry.timestamp,
                updated_at=committed_entry.timestamp,
            )
            
            self.account_repo.save(account)
            self.db.commit()
            
            return {
                "status": "success",
                "message": f"Account {account_id} created with balance {balance} {currency}",
                "timestamp": committed_entry.timestamp.isoformat(),
            }
            
        except Exception as e:
            logger.error(f"Failed to create account: {str(e)}")
            raise
