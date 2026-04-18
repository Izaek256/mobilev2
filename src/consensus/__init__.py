"""Raft Consensus Module - Production-ready distributed consensus with persistence.

This module provides:
1. RaftNode: Core Raft consensus implementation with disk persistence
2. StateM
achineApplier: Deterministic application of transactions with idempotency
3. ConsensusTransactionManager: Transaction coordination through consensus
4. SnapshotManager: Log compaction and recovery optimization

Example usage:

    from src.consensus.raft_node import initialize_raft_node
    from src.consensus.state_machine_applier import initialize_state_machine_applier
    from src.consensus.consensus_transaction_manager import initialize_consensus_transaction_manager
    from src.consensus.snapshot_manager import initialize_snapshot_manager
    
    # Initialize components
    node_id = "node-1"
    peers = ["http://node-2:8001", "http://node-3:8002"]
    
    raft_node = initialize_raft_node(node_id, peers)
    applier = initialize_state_machine_applier()
    manager = initialize_consensus_transaction_manager(raft_node, applier)
    snapshot_mgr = initialize_snapshot_manager(node_id)
    
    # Start the node
    await raft_node.start()
    
    # Submit transaction (only works if this is the leader)
    result = await manager.submit_transfer(
        from_account_id="alice",
        to_account_id="bob",
        amount="100",
        currency="USD",
        idempotency_key="tx-001"
    )
"""

from src.consensus.raft_node import (
    RaftNode,
    NodeRole,
    LogEntry,
    AppendEntriesRequest,
    AppendEntriesResponse,
    RequestVoteRequest,
    RequestVoteResponse,
    initialize_raft_node,
    get_raft_node,
)

from src.consensus.state_machine_applier import (
    StateMachineApplier,
    initialize_state_machine_applier,
    get_state_machine_applier,
)

from src.consensus.consensus_transaction_manager import (
    ConsensusTransactionManager,
    TransactionStatus,
    initialize_consensus_transaction_manager,
    get_consensus_transaction_manager,
)

from src.consensus.snapshot_manager import (
    SnapshotManager,
    initialize_snapshot_manager,
    get_snapshot_manager,
)

__all__ = [
    # RaftNode components
    "RaftNode",
    "NodeRole",
    "LogEntry",
    "AppendEntriesRequest",
    "AppendEntriesResponse",
    "RequestVoteRequest",
    "RequestVoteResponse",
    "initialize_raft_node",
    "get_raft_node",
    # State machine applier
    "StateMachineApplier",
    "initialize_state_machine_applier",
    "get_state_machine_applier",
    # Consensus transaction manager
    "ConsensusTransactionManager",
    "TransactionStatus",
    "initialize_consensus_transaction_manager",
    "get_consensus_transaction_manager",
    # Snapshot manager
    "SnapshotManager",
    "initialize_snapshot_manager",
    "get_snapshot_manager",
]
