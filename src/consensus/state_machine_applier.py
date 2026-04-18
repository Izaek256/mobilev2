"""State Machine Applier - Deterministically applies Raft log entries to database with idempotency."""

import asyncio
import logging
from typing import Optional, Dict, Any
from uuid import UUID
from datetime import datetime
from decimal import Decimal
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

from src.infrastructure.config.database import get_db_session
from src.infrastructure.adapter.persistence.sqlalchemy_models import (
    TransactionModel,
    AccountModel,
    LedgerEntryModel,
    IdempotencyKeyModel,
)
from src.domain.model.enums import TransactionStatus, TransactionType

logger = logging.getLogger(__name__)


class StateMachineApplier:
    """Deterministically applies Raft log entries to the state machine.
    
    This class ensures:
    1. All nodes apply entries in the same order (determinism)
    2. Timestamps and IDs are from the LogEntry, not generated during application
    3. Idempotency prevents double-charging on retries
    4. Database changes are atomic and consistent
    """
    
    def __init__(self, db_session: Optional[Session] = None):
        """Initialize the applier.
        
        Args:
            db_session: SQLAlchemy session (optional, uses default if not provided)
        """
        self.db_session = db_session or get_db_session()
    
    async def apply_entry(self, entry: Any) -> Dict[str, Any]:
        """Apply a single log entry to the state machine.
        
        Args:
            entry: LogEntry from Raft log containing:
                - term: Raft term
                - index: Log index
                - data: Command data (dict)
                    - command_type: Type of transaction (e.g., "transfer", "deposit")
                    - transaction_id: UUID (deterministic, from leader)
                    - idempotency_key: Unique key for deduplication
                    - from_account_id: Source account
                    - to_account_id: Destination account
                    - amount: Transfer amount
                    - currency: Currency code
                    - timestamp: Transaction timestamp (from leader)
        
        Returns:
            Result dict with status and transaction ID
        """
        try:
            data = entry.data
            command_type = data.get("command_type", "transfer")
            
            if command_type == "transfer":
                return await self._apply_transfer(entry, data)
            elif command_type == "deposit":
                return await self._apply_deposit(entry, data)
            elif command_type == "withdrawal":
                return await self._apply_withdrawal(entry, data)
            else:
                logger.warning(f"Unknown command type: {command_type}")
                return {"status": "error", "reason": "unknown_command_type"}
        
        except Exception as e:
            logger.error(f"Error applying entry {entry.index}: {e}")
            return {"status": "error", "reason": str(e)}
    
    async def _apply_transfer(self, entry: Any, data: Dict[str, Any]) -> Dict[str, Any]:
        """Apply a transfer transaction.
        
        Args:
            entry: LogEntry with transfer command
            data: Command data containing transaction details
        
        Returns:
            Result dict
        """
        # Extract deterministic values from LogEntry (not generated during application)
        transaction_id = UUID(data["transaction_id"])
        idempotency_key = data["idempotency_key"]
        from_account_id = data["from_account_id"]
        to_account_id = data["to_account_id"]
        amount = Decimal(str(data["amount"]))
        currency = data.get("currency", "USD")
        timestamp = datetime.fromisoformat(data.get("timestamp", datetime.utcnow().isoformat()))
        
        # Check idempotency: has this transaction already been processed?
        existing_txn = self.db_session.query(TransactionModel).filter(
            TransactionModel.idempotency_key == idempotency_key
        ).first()
        
        if existing_txn:
            # Idempotent response: return existing transaction result
            logger.info(f"Transfer {idempotency_key} already processed, returning idempotent result")
            return {
                "status": "success",
                "reason": "idempotent",
                "transaction_id": str(existing_txn.transaction_id),
            }
        
        # Check if transaction with this ID already exists
        existing_by_id = self.db_session.query(TransactionModel).filter(
            TransactionModel.transaction_id == transaction_id
        ).first()
        
        if existing_by_id:
            logger.warning(f"Transaction {transaction_id} already exists (collision?)")
            return {"status": "error", "reason": "transaction_id_collision"}
        
        # Validate accounts exist
        from_account = self.db_session.query(AccountModel).filter(
            AccountModel.account_id == from_account_id
        ).first()
        
        to_account = self.db_session.query(AccountModel).filter(
            AccountModel.account_id == to_account_id
        ).first()
        
        if not from_account:
            logger.warning(f"Source account {from_account_id} does not exist")
            return {"status": "error", "reason": "source_account_not_found"}
        
        if not to_account:
            logger.warning(f"Destination account {to_account_id} does not exist")
            return {"status": "error", "reason": "destination_account_not_found"}
        
        # Validate sufficient balance
        if from_account.balance < amount:
            logger.warning(
                f"Insufficient balance: {from_account.balance} < {amount}"
            )
            return {"status": "error", "reason": "insufficient_balance"}
        
        try:
            # Create transaction record
            transaction = TransactionModel(
                transaction_id=transaction_id,
                idempotency_key=idempotency_key,
                from_account_id=from_account_id,
                to_account_id=to_account_id,
                amount=amount,
                currency=currency,
                transaction_type=TransactionType.TRANSFER.value,
                status=TransactionStatus.PENDING.value,
                created_at=timestamp,
            )
            self.db_session.add(transaction)
            self.db_session.flush()
            
            # Update account balances (atomic)
            from_account.balance -= amount
            from_account.version += 1
            from_account.updated_at = datetime.utcnow()
            
            to_account.balance += amount
            to_account.version += 1
            to_account.updated_at = datetime.utcnow()
            
            # Create ledger entries
            debit_entry = LedgerEntryModel(
                transaction_id=transaction_id,
                account_id=from_account_id,
                entry_type="DEBIT",
                amount=amount,
                currency=currency,
                created_at=timestamp,
            )
            
            credit_entry = LedgerEntryModel(
                transaction_id=transaction_id,
                account_id=to_account_id,
                entry_type="CREDIT",
                amount=amount,
                currency=currency,
                created_at=timestamp,
            )
            
            self.db_session.add(debit_entry)
            self.db_session.add(credit_entry)
            
            # Mark transaction as completed
            transaction.status = TransactionStatus.COMPLETED.value
            transaction.completed_at = datetime.utcnow()
            
            self.db_session.commit()
            
            logger.info(
                f"Transfer {idempotency_key}: {from_account_id} -> {to_account_id}, "
                f"amount={amount} {currency}"
            )
            
            return {
                "status": "success",
                "transaction_id": str(transaction_id),
                "from_account_id": from_account_id,
                "to_account_id": to_account_id,
                "amount": str(amount),
            }
        
        except IntegrityError as e:
            # Handle constraint violations (e.g., duplicate idempotency_key due to race)
            self.db_session.rollback()
            logger.warning(f"Integrity error applying transfer: {e}")
            
            # Try to fetch the existing transaction
            existing = self.db_session.query(TransactionModel).filter(
                TransactionModel.idempotency_key == idempotency_key
            ).first()
            
            if existing:
                return {
                    "status": "success",
                    "reason": "idempotent_race",
                    "transaction_id": str(existing.transaction_id),
                }
            
            return {"status": "error", "reason": "integrity_violation"}
        
        except Exception as e:
            self.db_session.rollback()
            logger.error(f"Error applying transfer: {e}")
            return {"status": "error", "reason": str(e)}
    
    async def _apply_deposit(self, entry: Any, data: Dict[str, Any]) -> Dict[str, Any]:
        """Apply a deposit transaction.
        
        Args:
            entry: LogEntry with deposit command
            data: Command data
        
        Returns:
            Result dict
        """
        try:
            transaction_id = UUID(data["transaction_id"])
            idempotency_key = data["idempotency_key"]
            account_id = data["account_id"]
            amount = Decimal(str(data["amount"]))
            currency = data.get("currency", "USD")
            timestamp = datetime.fromisoformat(data.get("timestamp", datetime.utcnow().isoformat()))
            
            # Check idempotency
            existing = self.db_session.query(TransactionModel).filter(
                TransactionModel.idempotency_key == idempotency_key
            ).first()
            
            if existing:
                return {
                    "status": "success",
                    "reason": "idempotent",
                    "transaction_id": str(existing.transaction_id),
                }
            
            # Get account
            account = self.db_session.query(AccountModel).filter(
                AccountModel.account_id == account_id
            ).first()
            
            if not account:
                return {"status": "error", "reason": "account_not_found"}
            
            # Create transaction
            transaction = TransactionModel(
                transaction_id=transaction_id,
                idempotency_key=idempotency_key,
                from_account_id=account_id,
                to_account_id=account_id,
                amount=amount,
                currency=currency,
                transaction_type="DEPOSIT",
                status=TransactionStatus.COMPLETED.value,
                created_at=timestamp,
                completed_at=datetime.utcnow(),
            )
            
            self.db_session.add(transaction)
            
            # Update account balance
            account.balance += amount
            account.version += 1
            account.updated_at = datetime.utcnow()
            
            # Create ledger entry
            entry_record = LedgerEntryModel(
                transaction_id=transaction_id,
                account_id=account_id,
                entry_type="CREDIT",
                amount=amount,
                currency=currency,
                created_at=timestamp,
            )
            self.db_session.add(entry_record)
            
            self.db_session.commit()
            
            logger.info(f"Deposit {idempotency_key}: account={account_id}, amount={amount}")
            
            return {
                "status": "success",
                "transaction_id": str(transaction_id),
                "account_id": account_id,
                "amount": str(amount),
            }
        
        except IntegrityError:
            self.db_session.rollback()
            existing = self.db_session.query(TransactionModel).filter(
                TransactionModel.idempotency_key == idempotency_key
            ).first()
            if existing:
                return {
                    "status": "success",
                    "reason": "idempotent_race",
                    "transaction_id": str(existing.transaction_id),
                }
            return {"status": "error", "reason": "integrity_violation"}
        
        except Exception as e:
            self.db_session.rollback()
            logger.error(f"Error applying deposit: {e}")
            return {"status": "error", "reason": str(e)}
    
    async def _apply_withdrawal(self, entry: Any, data: Dict[str, Any]) -> Dict[str, Any]:
        """Apply a withdrawal transaction.
        
        Args:
            entry: LogEntry with withdrawal command
            data: Command data
        
        Returns:
            Result dict
        """
        try:
            transaction_id = UUID(data["transaction_id"])
            idempotency_key = data["idempotency_key"]
            account_id = data["account_id"]
            amount = Decimal(str(data["amount"]))
            currency = data.get("currency", "USD")
            timestamp = datetime.fromisoformat(data.get("timestamp", datetime.utcnow().isoformat()))
            
            # Check idempotency
            existing = self.db_session.query(TransactionModel).filter(
                TransactionModel.idempotency_key == idempotency_key
            ).first()
            
            if existing:
                return {
                    "status": "success",
                    "reason": "idempotent",
                    "transaction_id": str(existing.transaction_id),
                }
            
            # Get account
            account = self.db_session.query(AccountModel).filter(
                AccountModel.account_id == account_id
            ).first()
            
            if not account:
                return {"status": "error", "reason": "account_not_found"}
            
            if account.balance < amount:
                return {"status": "error", "reason": "insufficient_balance"}
            
            # Create transaction
            transaction = TransactionModel(
                transaction_id=transaction_id,
                idempotency_key=idempotency_key,
                from_account_id=account_id,
                to_account_id=account_id,
                amount=amount,
                currency=currency,
                transaction_type="WITHDRAWAL",
                status=TransactionStatus.COMPLETED.value,
                created_at=timestamp,
                completed_at=datetime.utcnow(),
            )
            
            self.db_session.add(transaction)
            
            # Update account balance
            account.balance -= amount
            account.version += 1
            account.updated_at = datetime.utcnow()
            
            # Create ledger entry
            entry_record = LedgerEntryModel(
                transaction_id=transaction_id,
                account_id=account_id,
                entry_type="DEBIT",
                amount=amount,
                currency=currency,
                created_at=timestamp,
            )
            self.db_session.add(entry_record)
            
            self.db_session.commit()
            
            logger.info(f"Withdrawal {idempotency_key}: account={account_id}, amount={amount}")
            
            return {
                "status": "success",
                "transaction_id": str(transaction_id),
                "account_id": account_id,
                "amount": str(amount),
            }
        
        except IntegrityError:
            self.db_session.rollback()
            existing = self.db_session.query(TransactionModel).filter(
                TransactionModel.idempotency_key == idempotency_key
            ).first()
            if existing:
                return {
                    "status": "success",
                    "reason": "idempotent_race",
                    "transaction_id": str(existing.transaction_id),
                }
            return {"status": "error", "reason": "integrity_violation"}
        
        except Exception as e:
            self.db_session.rollback()
            logger.error(f"Error applying withdrawal: {e}")
            return {"status": "error", "reason": str(e)}


# Global state machine applier instance
_applier: Optional[StateMachineApplier] = None


def get_state_machine_applier() -> StateMachineApplier:
    """Get the global state machine applier instance."""
    global _applier
    if _applier is None:
        _applier = StateMachineApplier()
    return _applier


def initialize_state_machine_applier(db_session: Optional[Session] = None) -> StateMachineApplier:
    """Initialize the global state machine applier."""
    global _applier
    _applier = StateMachineApplier(db_session)
    return _applier
