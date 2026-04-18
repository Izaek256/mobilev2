"""Consensus Transaction Manager - Coordinates between API layer and Raft consensus.

This manager ensures:
1. Leader generates deterministic transaction IDs and timestamps
2. Transactions are submitted to Raft log before being applied
3. Idempotency keys prevent double-processing
4. Clients wait for commitment before getting response
5. Followers reject transaction requests
"""

import asyncio
import logging
import uuid
from typing import Dict, Any, Optional, Callable
from datetime import datetime
from decimal import Decimal
from enum import Enum

from src.consensus.raft_node import RaftNode, NodeRole
from src.consensus.state_machine_applier import StateMachineApplier
from src.domain.model.enums import TransactionStatus

logger = logging.getLogger(__name__)


class TransactionStatus(str, Enum):
    """Transaction status in consensus system."""
    PENDING = "pending"
    SUBMITTED = "submitted"
    COMMITTED = "committed"
    APPLIED = "applied"
    FAILED = "failed"


class ConsensusTransactionManager:
    """Manages transaction flow through Raft consensus with idempotency.
    
    The transaction lifecycle:
    1. Client submits transaction request (with optional idempotency_key)
    2. Leader generates transaction_id and timestamp
    3. LogEntry is appended to leader's log
    4. Followers replicate the entry
    5. Entry is committed when majority has replicated it
    6. All nodes apply the entry to their state machine
    7. Client receives response when entry is committed (not necessarily applied yet)
    """
    
    def __init__(
        self,
        raft_node: RaftNode,
        state_machine_applier: StateMachineApplier,
    ):
        """Initialize the transaction manager.
        
        Args:
            raft_node: Raft consensus node
            state_machine_applier: State machine applier
        """
        self.raft_node = raft_node
        self.applier = state_machine_applier
        
        # Pending transactions waiting for commitment
        # Key: transaction_id, Value: asyncio.Event
        self.pending_transactions: Dict[str, asyncio.Event] = {}
        self.transaction_results: Dict[str, Dict[str, Any]] = {}
        
        # Register callback with Raft to be notified when entries are applied
        self.raft_node.apply_entry_callback = self._on_entry_applied
    
    async def submit_transfer(
        self,
        from_account_id: str,
        to_account_id: str,
        amount: str,
        currency: str = "USD",
        idempotency_key: Optional[str] = None,
        wait_for_commit: bool = True,
    ) -> Dict[str, Any]:
        """Submit a transfer transaction through Raft consensus.
        
        The leader generates the transaction_id and timestamp to ensure
        determinism across the cluster.
        
        Args:
            from_account_id: Source account ID
            to_account_id: Destination account ID
            amount: Transfer amount as string (to avoid float precision issues)
            currency: Currency code (default: USD)
            idempotency_key: Optional idempotency key for deduplication
            wait_for_commit: Wait for entry to be committed (default: True)
        
        Returns:
            Dict with transaction result:
                - status: success/error
                - transaction_id: Generated transaction ID
                - idempotency_key: The idempotency key used
                - log_index: Raft log index (if committed)
        
        Raises:
            RuntimeError: If node is not leader
        """
        if self.raft_node.role != NodeRole.LEADER:
            raise RuntimeError(
                f"Cannot submit transaction: node is {self.raft_node.role.value}, "
                "expected leader"
            )
        
        # Generate deterministic transaction ID and timestamp
        # These are deterministic so all nodes will use the same values
        transaction_id = str(uuid.uuid4())
        timestamp = datetime.utcnow().isoformat()
        
        # Use provided idempotency key or generate one
        if not idempotency_key:
            idempotency_key = f"{from_account_id}:{to_account_id}:{timestamp}"
        
        # Build the log entry data
        entry_data = {
            "command_type": "transfer",
            "transaction_id": transaction_id,
            "idempotency_key": idempotency_key,
            "from_account_id": from_account_id,
            "to_account_id": to_account_id,
            "amount": amount,
            "currency": currency,
            "timestamp": timestamp,
        }
        
        try:
            # Append to Raft log as leader
            log_index = self.raft_node.append_entry(entry_data)
            
            logger.info(
                f"Submitted transfer transaction: {transaction_id} "
                f"at log index {log_index}"
            )
            
            result = {
                "status": "submitted",
                "transaction_id": transaction_id,
                "idempotency_key": idempotency_key,
                "log_index": log_index,
            }
            
            if wait_for_commit:
                # Wait for the entry to be committed
                result = await self._wait_for_commit(log_index, transaction_id)
            
            return result
        
        except Exception as e:
            logger.error(f"Error submitting transfer: {e}")
            return {
                "status": "error",
                "reason": str(e),
                "idempotency_key": idempotency_key,
            }
    
    async def submit_deposit(
        self,
        account_id: str,
        amount: str,
        currency: str = "USD",
        idempotency_key: Optional[str] = None,
        wait_for_commit: bool = True,
    ) -> Dict[str, Any]:
        """Submit a deposit transaction through Raft consensus.
        
        Args:
            account_id: Account to deposit to
            amount: Deposit amount as string
            currency: Currency code
            idempotency_key: Optional idempotency key
            wait_for_commit: Wait for commitment
        
        Returns:
            Transaction result dict
        """
        if self.raft_node.role != NodeRole.LEADER:
            raise RuntimeError(
                f"Cannot submit transaction: node is {self.raft_node.role.value}"
            )
        
        transaction_id = str(uuid.uuid4())
        timestamp = datetime.utcnow().isoformat()
        
        if not idempotency_key:
            idempotency_key = f"{account_id}:deposit:{timestamp}"
        
        entry_data = {
            "command_type": "deposit",
            "transaction_id": transaction_id,
            "idempotency_key": idempotency_key,
            "account_id": account_id,
            "amount": amount,
            "currency": currency,
            "timestamp": timestamp,
        }
        
        try:
            log_index = self.raft_node.append_entry(entry_data)
            
            logger.info(
                f"Submitted deposit transaction: {transaction_id} "
                f"at log index {log_index}"
            )
            
            result = {
                "status": "submitted",
                "transaction_id": transaction_id,
                "idempotency_key": idempotency_key,
                "log_index": log_index,
            }
            
            if wait_for_commit:
                result = await self._wait_for_commit(log_index, transaction_id)
            
            return result
        
        except Exception as e:
            logger.error(f"Error submitting deposit: {e}")
            return {
                "status": "error",
                "reason": str(e),
                "idempotency_key": idempotency_key,
            }
    
    async def submit_withdrawal(
        self,
        account_id: str,
        amount: str,
        currency: str = "USD",
        idempotency_key: Optional[str] = None,
        wait_for_commit: bool = True,
    ) -> Dict[str, Any]:
        """Submit a withdrawal transaction through Raft consensus.
        
        Args:
            account_id: Account to withdraw from
            amount: Withdrawal amount as string
            currency: Currency code
            idempotency_key: Optional idempotency key
            wait_for_commit: Wait for commitment
        
        Returns:
            Transaction result dict
        """
        if self.raft_node.role != NodeRole.LEADER:
            raise RuntimeError(
                f"Cannot submit transaction: node is {self.raft_node.role.value}"
            )
        
        transaction_id = str(uuid.uuid4())
        timestamp = datetime.utcnow().isoformat()
        
        if not idempotency_key:
            idempotency_key = f"{account_id}:withdrawal:{timestamp}"
        
        entry_data = {
            "command_type": "withdrawal",
            "transaction_id": transaction_id,
            "idempotency_key": idempotency_key,
            "account_id": account_id,
            "amount": amount,
            "currency": currency,
            "timestamp": timestamp,
        }
        
        try:
            log_index = self.raft_node.append_entry(entry_data)
            
            logger.info(
                f"Submitted withdrawal transaction: {transaction_id} "
                f"at log index {log_index}"
            )
            
            result = {
                "status": "submitted",
                "transaction_id": transaction_id,
                "idempotency_key": idempotency_key,
                "log_index": log_index,
            }
            
            if wait_for_commit:
                result = await self._wait_for_commit(log_index, transaction_id)
            
            return result
        
        except Exception as e:
            logger.error(f"Error submitting withdrawal: {e}")
            return {
                "status": "error",
                "reason": str(e),
                "idempotency_key": idempotency_key,
            }
    
    async def _wait_for_commit(
        self,
        log_index: int,
        transaction_id: str,
        timeout_seconds: float = 5.0,
    ) -> Dict[str, Any]:
        """Wait for a log entry to be committed.
        
        Args:
            log_index: Log index to wait for
            transaction_id: Transaction ID for tracking
            timeout_seconds: Timeout in seconds
        
        Returns:
            Transaction result dict with committed status
        """
        # Create event to wait on
        event = asyncio.Event()
        self.pending_transactions[transaction_id] = event
        
        try:
            # Wait for commit with timeout
            await asyncio.wait_for(event.wait(), timeout=timeout_seconds)
            
            # Retrieve result
            result = self.transaction_results.get(
                transaction_id,
                {"status": "committed", "transaction_id": transaction_id},
            )
            
            return result
        
        except asyncio.TimeoutError:
            logger.warning(
                f"Timeout waiting for transaction {transaction_id} "
                f"at log index {log_index}"
            )
            return {
                "status": "timeout",
                "reason": f"Did not commit within {timeout_seconds} seconds",
                "transaction_id": transaction_id,
                "log_index": log_index,
            }
        
        finally:
            # Clean up
            self.pending_transactions.pop(transaction_id, None)
            self.transaction_results.pop(transaction_id, None)
    
    async def _on_entry_applied(self, entry: Any) -> None:
        """Callback when Raft applies an entry to state machine.
        
        Args:
            entry: LogEntry that was applied
        """
        try:
            # Apply to state machine
            result = await self.applier.apply_entry(entry)
            
            # Notify waiting clients
            data = entry.data
            transaction_id = data.get("transaction_id")
            
            if transaction_id and transaction_id in self.pending_transactions:
                self.transaction_results[transaction_id] = result
                self.pending_transactions[transaction_id].set()
            
            logger.debug(f"Applied entry {entry.index}: {result}")
        
        except Exception as e:
            logger.error(f"Error in entry applied callback: {e}")


# Global instance
_manager: Optional[ConsensusTransactionManager] = None


def get_consensus_transaction_manager() -> ConsensusTransactionManager:
    """Get the global transaction manager."""
    global _manager
    if _manager is None:
        raise RuntimeError(
            "Consensus transaction manager not initialized. "
            "Call initialize_consensus_transaction_manager() first."
        )
    return _manager


def initialize_consensus_transaction_manager(
    raft_node: RaftNode,
    applier: StateMachineApplier,
) -> ConsensusTransactionManager:
    """Initialize the global transaction manager."""
    global _manager
    _manager = ConsensusTransactionManager(raft_node, applier)
    return _manager
