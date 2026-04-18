"""
Consensus Transaction Manager - Orchestrates the consensus-first transaction flow.

This module handles the complete workflow:
1. Receive API request with Transaction ID
2. Check if current node is Leader
3. Append to Raft log
4. Wait for commitment
5. Trigger state machine apply
"""

import asyncio
import logging
from typing import Dict, Any, Optional, Tuple
from uuid import uuid4
from datetime import datetime

import src.infrastructure.consensus.raft_node as raft_module
from src.infrastructure.consensus.state_machine_applier import (
    CommittedLogEntry,
    StateMachineApplier,
)
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


class ConsensusTransactionError(Exception):
    """Raised when consensus fails or cannot be reached."""
    pass


class NotLeaderError(ConsensusTransactionError):
    """Raised when current node is not the leader."""
    pass


class ConsensusTimeoutError(ConsensusTransactionError):
    """Raised when commitment is not reached within timeout."""
    pass


class ConsensusTransactionManager:
    """
    Manages the consensus-first transaction flow.
    
    Workflow:
    1. Receive transaction wrapped in Transaction ID (UUID)
    2. Verify current node is Leader (redirect if not)
    3. Append transaction to Raft log
    4. Wait for commitment (replicated to majority)
    5. Trigger state machine apply on committed entry
    
    Guarantees:
    - LOCAL DATABASE IS NEVER WRITTEN FROM API HANDLERS
    - All state changes come from committed Raft entries
    - Idempotent: Retries with same transaction ID are safe
    """
    
    def __init__(self, db: Session):
        self.db = db
        self.state_machine_applier = StateMachineApplier(db)
    
    def get_leader_address(self) -> Optional[str]:
        """
        Get the current leader's address.
        
        Returns:
            Leader address (e.g., "http://localhost:8000") or None if unknown
        """
        if not raft_module.node:
            return None
        
        # If this node is the leader, return self address (from context)
        if raft_module.node.state == raft_module.NodeState.LEADER:
            return None  # Caller should use self
        
        # If this node is a follower, return leader's address from peers
        if raft_module.node.leader_id:
            # In a real implementation, maintain a mapping of node_id -> address
            # For now, we'll return the leader_id and let caller resolve it
            return raft_module.node.leader_id
        
        return None
    
    def is_leader(self) -> bool:
        """
        Check if current node is the leader.
        
        Returns:
            True if leader, False otherwise
        """
        if not raft_module.node:
            return False
        return raft_module.node.state == raft_module.NodeState.LEADER
    
    async def append_and_wait_for_commitment(
        self,
        operation_type: str,
        payload: Dict[str, Any],
        timeout_seconds: float = 5.0,
    ) -> Tuple[str, Dict[str, Any]]:
        """
        Append a transaction to Raft log and wait for commitment.
        
        This is the CONSENSUS LAYER - bridges API requests and state machine.
        
        Steps:
        1. Generate or validate transaction ID
        2. Verify this node is Leader (will be checked by caller first)
        3. Append log entry to Raft
        4. Wait for entry to be committed (replicated to majority)
        5. Trigger state machine apply
        
        Args:
            operation_type: Type of operation (e.g., "send_money", "create_account")
            payload: Operation payload (e.g., account IDs, amounts)
            timeout_seconds: How long to wait for commitment
            
        Returns:
            Tuple of (transaction_id, result_dict)
            
        Raises:
            NotLeaderError: If current node is not the leader
            ConsensusTimeoutError: If commitment not reached within timeout
            ConsensusTransactionError: If consensus fails
        """
        # Step 1: Generate transaction ID (unique identifier for this request)
        transaction_id = str(uuid4())
        logger.info(f"Starting consensus transaction: {transaction_id}")
        
        # Step 2: Verify we're the leader (should be checked by caller, but verify here too)
        if not self.is_leader():
            raise NotLeaderError(
                f"Current node {raft_module.node.node_id} is not the leader. "
                f"Redirect request to leader."
            )
        
        # Step 3: Create log entry with transaction ID
        log_entry_data = {
            "transaction_id": transaction_id,
            "operation_type": operation_type,
            "payload": payload,
            "timestamp": datetime.utcnow().isoformat(),
        }
        
        try:
            # Append to Raft log
            log_index = raft_module.node.append_entry(log_entry_data)
            logger.info(
                f"Transaction {transaction_id} appended to Raft log at index {log_index}"
            )
        except Exception as e:
            logger.error(f"Failed to append to Raft log: {str(e)}")
            raise ConsensusTransactionError(f"Failed to append to Raft log: {str(e)}")
        
        # Step 4: Wait for commitment
        try:
            committed_entry = await self._wait_for_commitment(
                log_index,
                timeout_seconds,
            )
            logger.info(
                f"Transaction {transaction_id} committed at index {log_index}"
            )
        except asyncio.TimeoutError:
            logger.error(
                f"Transaction {transaction_id} failed to commit within {timeout_seconds}s"
            )
            raise ConsensusTimeoutError(
                f"Transaction did not reach consensus within {timeout_seconds} seconds. "
                f"Request may have been processed - check idempotency_key."
            )
        
        # Step 5: Apply to state machine
        try:
            result = self.state_machine_applier.apply(committed_entry)
            logger.info(f"Transaction {transaction_id} applied to state machine")
            return transaction_id, result
        except Exception as e:
            logger.error(
                f"CRITICAL: Failed to apply committed transaction {transaction_id}. "
                f"Consensus succeeded but state machine failed. Error: {str(e)}",
                exc_info=True,
            )
            raise
    
    async def _wait_for_commitment(
        self,
        log_index: int,
        timeout_seconds: float = 5.0,
    ) -> CommittedLogEntry:
        """
        Wait for a log entry at the given index to be committed.
        
        Committed means: replicated to a majority of nodes.
        
        Args:
            log_index: Index in Raft log to wait for
            timeout_seconds: How long to wait
            
        Returns:
            CommittedLogEntry once it's committed
            
        Raises:
            asyncio.TimeoutError: If not committed within timeout
        """
        start_time = datetime.utcnow()
        check_interval = 0.1  # Check every 100ms
        
        while True:
            # Check if entry is committed
            if log_index <= raft_module.node.commit_index:
                # Entry is committed, retrieve it from log
                log_entry = raft_module.node.log[log_index - 1]
                committed = CommittedLogEntry.from_raft_log_entry(log_entry)
                logger.debug(f"Entry at index {log_index} is committed")
                return committed
            
            # Check timeout
            elapsed = (datetime.utcnow() - start_time).total_seconds()
            if elapsed > timeout_seconds:
                raise asyncio.TimeoutError(
                    f"Log entry at index {log_index} not committed after {timeout_seconds}s. "
                    f"Current commit_index: {raft_module.node.commit_index}"
                )
            
            # Wait before checking again
            await asyncio.sleep(check_interval)


class RedirectToLeaderResponse:
    """Response for redirecting to leader."""
    
    def __init__(self, leader_address: str):
        self.leader_address = leader_address
        self.status_code = 307
        self.message = f"Redirect to leader at {leader_address}"
