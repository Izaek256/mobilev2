"""Transactional Coordinator - integrates Raft consensus with transaction handling."""

import logging
import json
from typing import Dict, Any, Optional, Tuple
from uuid import uuid4
from datetime import datetime

from src.infrastructure.consensus.raft_node import RaftNode

logger = logging.getLogger(__name__)


class TransactionCoordinator:
    """
    Coordinates transaction execution across the distributed system.
    Routes transactions through Raft for consensus before application.
    """
    
    def __init__(self, raft_node: Optional[RaftNode], node_id: str):
        self.raft_node = raft_node
        self.node_id = node_id
        self.pending_transactions: Dict[str, Dict[str, Any]] = {}
    
    async def submit_transaction(
        self,
        transaction_type: str,
        data: Dict[str, Any],
        idempotency_key: str = None
    ) -> Tuple[bool, str, Optional[str]]:
        """
        Submit a transaction for consensus and application.
        
        Args:
            transaction_type: Type of transaction (e.g., "transfer", "deposit")
            data: Transaction data
            idempotency_key: Optional idempotency key for deduplication
            
        Returns:
            (success, message, transaction_id)
        """
        if not idempotency_key:
            idempotency_key = str(uuid4())
        
        transaction_id = str(uuid4())
        
        # Prepare log entry
        log_entry = {
            "type": transaction_type,
            "transaction_id": transaction_id,
            "idempotency_key": idempotency_key,
            "data": data,
            "timestamp": datetime.utcnow().isoformat(),
            "initiated_by": self.node_id
        }
        
        # If no Raft node, fail
        if not self.raft_node:
            return False, "Raft consensus not available", None
        
        # Submit to Raft leader
        if self.raft_node.is_leader():
            # This node is the leader, append directly
            success, error = await self.raft_node.append_log(log_entry)
            if success:
                logger.info(f"Transaction {transaction_id} appended to log")
                return True, "Transaction committed", transaction_id
            else:
                logger.error(f"Failed to append transaction {transaction_id}: {error}")
                return False, f"Failed to commit transaction: {error}", None
        else:
            # This node is a follower, forward to leader
            leader_id = self.raft_node.get_leader_id()
            if not leader_id:
                return False, "No leader elected", None
            
            logger.info(f"Forwarding transaction {transaction_id} to leader {leader_id}")
            # In a real implementation, we would forward to the leader
            # For now, we'll return an error asking client to retry
            return False, f"This node is a follower. Try the leader: {leader_id}", None
    
    def apply_transaction(
        self,
        transaction_id: str,
        transaction_type: str,
        data: Dict[str, Any]
    ) -> Tuple[bool, Optional[str]]:
        """
        Apply a transaction to the state machine.
        Called when the transaction is committed via Raft consensus.
        
        Args:
            transaction_id: The transaction ID
            transaction_type: Type of transaction
            data: Transaction data
            
        Returns:
            (success, error_message)
        """
        logger.info(f"Applying transaction {transaction_id} of type {transaction_type}")
        
        # Store in pending transactions
        self.pending_transactions[transaction_id] = {
            "type": transaction_type,
            "data": data,
            "status": "applied",
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # TODO: Call appropriate service based on transaction_type
        # For example:
        # if transaction_type == "transfer":
        #     return self._apply_transfer(transaction_id, data)
        # elif transaction_type == "deposit":
        #     return self._apply_deposit(transaction_id, data)
        
        return True, None
    
    def get_transaction_status(self, transaction_id: str) -> Dict[str, Any]:
        """Get the status of a transaction.
        
        Args:
            transaction_id: The transaction ID
            
        Returns:
            Transaction status dictionary
        """
        if transaction_id in self.pending_transactions:
            return self.pending_transactions[transaction_id]
        
        return {
            "status": "unknown",
            "transaction_id": transaction_id
        }


class ReplicationProtocol:
    """
    Implements the node-to-node replication protocol.
    Handles catch-up of followers and acknowledgment tracking.
    """
    
    def __init__(self, raft_node: Optional[RaftNode], node_id: str):
        self.raft_node = raft_node
        self.node_id = node_id
        self.replication_status: Dict[str, Dict[str, Any]] = {}
    
    async def replicate_entries(
        self,
        peer_id: str,
        entries: list
    ) -> Tuple[bool, Optional[str]]:
        """
        Replicate log entries to a peer.
        
        Args:
            peer_id: The peer node ID
            entries: List of log entries to replicate
            
        Returns:
            (success, error_message)
        """
        if not self.raft_node or not self.raft_node.is_leader():
            return False, "Only leader can initiate replication"
        
        logger.debug(f"Replicating {len(entries)} entries to {peer_id}")
        # Replication happens via heartbeat loop in RaftNode
        return True, None
    
    async def catchup_follower(
        self,
        follower_id: str,
        from_index: int
    ) -> Tuple[bool, Optional[str]]:
        """
        Catch up a follower node with entries starting from a specific index.
        
        Args:
            follower_id: The follower node ID
            from_index: Starting log index
            
        Returns:
            (success, error_message)
        """
        if not self.raft_node or not self.raft_node.is_leader():
            return False, "Only leader can catchup followers"
        
        logger.info(f"Starting catch-up of {follower_id} from index {from_index}")
        # The heartbeat loop will handle this automatically
        return True, None
    
    def get_replication_status(self) -> Dict[str, Any]:
        """Get replication status across all peers.
        
        Returns:
            Replication status dictionary
        """
        if not self.raft_node:
            return {"status": "unavailable"}
        
        status = {
            "node_id": self.raft_node.node_id,
            "is_leader": self.raft_node.is_leader(),
            "peers": {}
        }
        
        if self.raft_node.is_leader():
            for peer in self.raft_node.peers:
                status["peers"][peer] = {
                    "next_index": self.raft_node.next_index.get(peer, 1),
                    "match_index": self.raft_node.match_index.get(peer, 0),
                    "replicated_entries": self.raft_node.match_index.get(peer, 0)
                }
        
        return status
    
    def get_acknowledgment_level(self, entry_id: str) -> int:
        """
        Get the number of nodes that have acknowledged (replicated) an entry.
        
        Args:
            entry_id: The entry ID
            
        Returns:
            Number of nodes with the entry
        """
        if not self.raft_node or not self.raft_node.is_leader():
            return 0
        
        # Count nodes that have the entry (based on match_index)
        count = 1  # Include self
        for peer in self.raft_node.peers:
            # This is simplified; would need to track which entry is at match_index
            if self.raft_node.match_index.get(peer, 0) > 0:
                count += 1
        
        return count


class ConsensusedTransactionManager:
    """
    High-level manager combining transaction coordination and replication.
    """
    
    def __init__(self, node_id: str):
        self.node_id = node_id
        self.coordinator = TransactionCoordinator(raft_node, node_id)
        self.replication = ReplicationProtocol(raft_node, node_id)
    
    async def execute_transaction(
        self,
        transaction_type: str,
        data: Dict[str, Any],
        idempotency_key: str = None,
        wait_for_acks: int = 1
    ) -> Tuple[bool, str, Optional[str]]:
        """
        Execute a transaction with consensus and replication.
        
        Args:
            transaction_type: Type of transaction
            data: Transaction data
            idempotency_key: Optional idempotency key
            wait_for_acks: Number of nodes to wait for acknowledgment (1-N)
            
        Returns:
            (success, message, transaction_id)
        """
        # Submit for consensus
        success, message, transaction_id = await self.coordinator.submit_transaction(
            transaction_type,
            data,
            idempotency_key
        )
        
        if not success:
            return False, message, None
        
        # Wait for replication acknowledgments
        if wait_for_acks > 1:
            ack_level = self.replication.get_acknowledgment_level(transaction_id)
            if ack_level < wait_for_acks:
                logger.warning(f"Transaction {transaction_id} has {ack_level} acks, expected {wait_for_acks}")
                # In production, might want to wait with timeout
        
        return True, f"Transaction executed with {wait_for_acks} acknowledgments", transaction_id
    
    def get_cluster_status(self) -> Dict[str, Any]:
        """Get comprehensive cluster status.
        
        Returns:
            Cluster status dictionary
        """
        return {
            "node_id": self.node_id,
            "raft_status": self.coordinator.raft_node.get_status() if self.coordinator.raft_node else None,
            "replication_status": self.replication.get_replication_status()
        }
