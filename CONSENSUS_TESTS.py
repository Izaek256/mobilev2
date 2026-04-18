"""Test suite for verifying persistence, idempotency, and snapshotting.

This file demonstrates how to test the production-ready features:
1. Disk persistence (WAL survives restarts)
2. Idempotency (no double charges)
3. Snapshotting (log compaction works)
4. Deterministic application (all nodes compute same state)
"""

import asyncio
import pytest
from decimal import Decimal
from sqlalchemy.orm import Session

# Imports from consensus module
from src.consensus.raft_node import (
    initialize_raft_node,
    get_raft_node,
    NodeRole,
    RaftNode,
)
from src.consensus.state_machine_applier import (
    initialize_state_machine_applier,
    get_state_machine_applier,
)
from src.consensus.consensus_transaction_manager import (
    initialize_consensus_transaction_manager,
    get_consensus_transaction_manager,
)
from src.consensus.snapshot_manager import (
    initialize_snapshot_manager,
    get_snapshot_manager,
)

# Database imports
from src.infrastructure.config.database import get_db_session, init_db
from src.infrastructure.adapter.persistence.sqlalchemy_models import (
    AccountModel,
    TransactionModel,
)


class TestPersistence:
    """Test that Raft log entries survive node restarts."""
    
    @pytest.fixture
    def db_session(self):
        """Get a database session."""
        init_db()
        return get_db_session()
    
    @pytest.fixture
    def setup_accounts(self, db_session: Session):
        """Create test accounts."""
        alice = AccountModel(
            account_id="alice",
            owner_id="user1",
            balance=Decimal("1000"),
            currency="USD",
        )
        bob = AccountModel(
            account_id="bob",
            owner_id="user2",
            balance=Decimal("0"),
            currency="USD",
        )
        db_session.add(alice)
        db_session.add(bob)
        db_session.commit()
    
    @pytest.mark.asyncio
    async def test_wal_persistence(self, db_session: Session, setup_accounts):
        """Test that log entries are persisted to WAL."""
        
        # Initialize Raft node
        raft_node = initialize_raft_node(
            node_id="test-node-1",
            peers=[],
            election_timeout_ms=150,
        )
        
        # Start node
        await raft_node.start()
        
        # Force into leader state for testing
        raft_node.role = NodeRole.LEADER
        raft_node.leader_id = "test-node-1"
        
        # Append some entries
        entry1_index = raft_node.append_entry({
            "command_type": "transfer",
            "transaction_id": "tx-001",
            "idempotency_key": "idempotency-001",
            "from_account_id": "alice",
            "to_account_id": "bob",
            "amount": "100",
            "currency": "USD",
            "timestamp": "2024-01-01T12:00:00",
        })
        
        entry2_index = raft_node.append_entry({
            "command_type": "transfer",
            "transaction_id": "tx-002",
            "idempotency_key": "idempotency-002",
            "from_account_id": "bob",
            "to_account_id": "alice",
            "amount": "50",
            "currency": "USD",
            "timestamp": "2024-01-01T12:00:01",
        })
        
        assert entry1_index == 1
        assert entry2_index == 2
        
        # Verify entries are in WAL
        entries = raft_node.wal.get_entries(from_index=1)
        assert len(entries) == 2
        assert entries[0]["data"]["transaction_id"] == "tx-001"
        assert entries[1]["data"]["transaction_id"] == "tx-002"
        
        # Stop node
        await raft_node.stop()
        
        # Create new node (simulating restart)
        raft_node_restarted = initialize_raft_node(
            node_id="test-node-1",
            peers=[],
            election_timeout_ms=150,
        )
        
        # Verify entries are restored from WAL
        assert len(raft_node_restarted.log) == 2
        assert raft_node_restarted.log[0].data["transaction_id"] == "tx-001"
        assert raft_node_restarted.log[1].data["transaction_id"] == "tx-002"
        
        await raft_node_restarted.stop()


class TestIdempotency:
    """Test that idempotency prevents double charges."""
    
    @pytest.fixture
    def db_session(self):
        """Get a database session."""
        init_db()
        return get_db_session()
    
    @pytest.fixture
    def setup_accounts(self, db_session: Session):
        """Create test accounts."""
        alice = AccountModel(
            account_id="alice",
            owner_id="user1",
            balance=Decimal("1000"),
            currency="USD",
        )
        bob = AccountModel(
            account_id="bob",
            owner_id="user2",
            balance=Decimal("0"),
            currency="USD",
        )
        db_session.add(alice)
        db_session.add(bob)
        db_session.commit()
    
    @pytest.mark.asyncio
    async def test_duplicate_transaction_is_idempotent(
        self,
        db_session: Session,
        setup_accounts,
    ):
        """Test that duplicate transactions with same idempotency_key return same result."""
        
        # Initialize consensus components
        raft_node = initialize_raft_node(
            node_id="test-node-1",
            peers=[],
        )
        
        applier = initialize_state_machine_applier(db_session)
        manager = initialize_consensus_transaction_manager(raft_node, applier)
        
        await raft_node.start()
        
        # Force into leader state
        raft_node.role = NodeRole.LEADER
        raft_node.leader_id = "test-node-1"
        
        # Submit first transfer
        result1 = await manager.submit_transfer(
            from_account_id="alice",
            to_account_id="bob",
            amount="100",
            idempotency_key="idempotency-test-001",
            wait_for_commit=False,
        )
        
        assert result1["status"] == "submitted"
        tx_id_1 = result1["transaction_id"]
        
        # Simulate entry being applied
        log_index = result1["log_index"]
        entry = raft_node._get_entry_at_index(log_index)
        if entry:
            await applier.apply_entry(entry)
        
        # Verify transaction was created
        txn = db_session.query(TransactionModel).filter(
            TransactionModel.idempotency_key == "idempotency-test-001"
        ).first()
        assert txn is not None
        assert txn.transaction_id == tx_id_1
        
        # Get original balance
        alice_before = db_session.query(AccountModel).filter(
            AccountModel.account_id == "alice"
        ).first()
        bob_before = db_session.query(AccountModel).filter(
            AccountModel.account_id == "bob"
        ).first()
        
        alice_balance_before = alice_before.balance
        bob_balance_before = bob_before.balance
        
        # Submit duplicate transfer with same idempotency_key
        result2 = await manager.submit_transfer(
            from_account_id="alice",
            to_account_id="bob",
            amount="100",
            idempotency_key="idempotency-test-001",
            wait_for_commit=False,
        )
        
        # Should return the same transaction ID
        assert result2["transaction_id"] == tx_id_1
        
        # Simulate entry being applied (again)
        log_index_2 = result2["log_index"]
        entry_2 = raft_node._get_entry_at_index(log_index_2)
        if entry_2:
            await applier.apply_entry(entry_2)
        
        # Verify balances didn't change (idempotent)
        alice_after = db_session.query(AccountModel).filter(
            AccountModel.account_id == "alice"
        ).first()
        bob_after = db_session.query(AccountModel).filter(
            AccountModel.account_id == "bob"
        ).first()
        
        # Balances should be the same as they were after first transfer
        # (not double-charged)
        assert alice_after.balance == alice_balance_before
        assert bob_after.balance == bob_balance_before
        
        await raft_node.stop()


class TestSnapshotting:
    """Test that snapshotting works correctly."""
    
    @pytest.fixture
    def db_session(self):
        """Get a database session."""
        init_db()
        return get_db_session()
    
    @pytest.fixture
    def setup_accounts(self, db_session: Session):
        """Create test accounts."""
        for i in range(10):
            account = AccountModel(
                account_id=f"account-{i}",
                owner_id=f"user-{i}",
                balance=Decimal("1000"),
                currency="USD",
            )
            db_session.add(account)
        db_session.commit()
    
    @pytest.mark.asyncio
    async def test_snapshot_creation_and_truncation(
        self,
        db_session: Session,
        setup_accounts,
    ):
        """Test that snapshots are created and logs are truncated."""
        
        # Initialize components
        raft_node = initialize_raft_node(
            node_id="test-node-1",
            peers=[],
        )
        
        snapshot_mgr = initialize_snapshot_manager(
            node_id="test-node-1",
            db_session=db_session,
            wal=raft_node.wal,
            snapshot_threshold=5,  # Low threshold for testing
        )
        
        await raft_node.start()
        
        # Force into leader state
        raft_node.role = NodeRole.LEADER
        raft_node.leader_id = "test-node-1"
        
        # Append 10 entries
        for i in range(10):
            raft_node.append_entry({
                "command_type": "transfer",
                "transaction_id": f"tx-{i}",
                "idempotency_key": f"idempotency-{i}",
                "from_account_id": f"account-{i % 10}",
                "to_account_id": f"account-{(i + 1) % 10}",
                "amount": "10",
                "currency": "USD",
                "timestamp": f"2024-01-01T12:00:{i:02d}",
            })
        
        # Verify 10 entries in log
        assert len(raft_node.log) == 10
        assert raft_node.wal.get_last_index() == 10
        
        # Simulate applying all entries
        raft_node.last_applied = 10
        raft_node.commit_index = 10
        
        # Create snapshot
        assert await snapshot_mgr.should_create_snapshot(10) == True
        
        metadata = await snapshot_mgr.create_snapshot(
            snapshot_index=10,
            snapshot_term=1,
        )
        
        assert metadata["snapshot_index"] == 10
        assert metadata["accounts_count"] == 10
        
        # Truncate log
        deleted = await snapshot_mgr.truncate_log(up_to_index=10)
        assert deleted == 10
        
        # Verify log is empty (all entries were before snapshot)
        remaining = raft_node.wal.get_entries(from_index=1)
        assert len(remaining) == 0
        
        await raft_node.stop()


class TestDeterministicApplication:
    """Test that state machine application is deterministic."""
    
    @pytest.fixture
    def db_session(self):
        """Get a database session."""
        init_db()
        return get_db_session()
    
    @pytest.fixture
    def setup_accounts(self, db_session: Session):
        """Create test accounts."""
        alice = AccountModel(
            account_id="alice",
            owner_id="user1",
            balance=Decimal("1000"),
            currency="USD",
        )
        bob = AccountModel(
            account_id="bob",
            owner_id="user2",
            balance=Decimal("0"),
            currency="USD",
        )
        db_session.add(alice)
        db_session.add(bob)
        db_session.commit()
    
    @pytest.mark.asyncio
    async def test_same_entry_produces_same_result(
        self,
        db_session: Session,
        setup_accounts,
    ):
        """Test that applying the same entry twice produces the same result."""
        
        applier1 = initialize_state_machine_applier(db_session)
        
        # Create a log entry with deterministic values from leader
        class MockLogEntry:
            def __init__(self, data):
                self.data = data
                self.index = 1
        
        entry_data = {
            "command_type": "transfer",
            "transaction_id": "tx-deterministic-001",
            "idempotency_key": "idempotency-deterministic-001",
            "from_account_id": "alice",
            "to_account_id": "bob",
            "amount": "100",
            "currency": "USD",
            "timestamp": "2024-01-01T12:00:00",  # Deterministic timestamp from leader
        }
        
        entry = MockLogEntry(entry_data)
        
        # Apply on node 1
        result1 = await applier1.apply_entry(entry)
        
        # Get balances after first application
        alice1 = db_session.query(AccountModel).filter(
            AccountModel.account_id == "alice"
        ).first()
        bob1 = db_session.query(AccountModel).filter(
            AccountModel.account_id == "bob"
        ).first()
        
        alice1_balance = alice1.balance
        bob1_balance = bob1.balance
        
        # Create second applier (simulating different node)
        db_session2 = get_db_session()
        applier2 = initialize_state_machine_applier(db_session2)
        
        # Get balances before second application
        alice_before = db_session2.query(AccountModel).filter(
            AccountModel.account_id == "alice"
        ).first()
        bob_before = db_session2.query(AccountModel).filter(
            AccountModel.account_id == "bob"
        ).first()
        
        alice_before_balance = alice_before.balance
        bob_before_balance = bob_before.balance
        
        # Apply same entry on node 2 (with idempotency, should be idempotent)
        result2 = await applier2.apply_entry(entry)
        
        # Results should be the same
        assert result1["status"] == result2["status"]
        assert result1["transaction_id"] == result2["transaction_id"]
        assert result1["reason"] == "idempotent"  # Second should be idempotent
        
        # Second node should return idempotent result without changing balances
        alice_after = db_session2.query(AccountModel).filter(
            AccountModel.account_id == "alice"
        ).first()
        bob_after = db_session2.query(AccountModel).filter(
            AccountModel.account_id == "bob"
        ).first()
        
        assert alice_after.balance == alice_before_balance
        assert bob_after.balance == bob_before_balance


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v", "-s"])
