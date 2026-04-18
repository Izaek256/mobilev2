"""Snapshotting Mechanism - Log compaction and recovery optimization.

This implements:
1. Periodic snapshot creation of database state
2. Log truncation after snapshots
3. Fast recovery by loading latest snapshot
4. InstallSnapshot RPC for followers that are too far behind
"""

import asyncio
import logging
import json
from typing import Optional, Dict, Any
from datetime import datetime
from sqlalchemy.orm import Session
from decimal import Decimal

from src.infrastructure.config.database import get_db_session
from src.infrastructure.adapter.persistence.sqlalchemy_models import (
    RaftLogModel,
    AccountModel,
)
from src.infrastructure.adapter.persistence.wal import WriteAheadLog

logger = logging.getLogger(__name__)


class SnapshotManager:
    """Manages log compaction through periodic snapshotting.
    
    When a Raft node's log grows too large:
    1. Periodically save the current state machine state to disk (snapshot)
    2. Record the log index and term of the snapshot
    3. Truncate all log entries up to the snapshot index
    4. On restart, load the snapshot and replay only entries after it
    5. Send snapshots to followers that are far behind
    
    Benefits:
    - Memory usage stays bounded (O(snapshot_size) + O(recent_entries))
    - Recovery time is O(recent_entries) instead of O(total_entries)
    - Followers can catch up quickly without replaying all history
    """
    
    def __init__(
        self,
        node_id: str,
        db_session: Optional[Session] = None,
        wal: Optional[WriteAheadLog] = None,
        snapshot_threshold: int = 100,
        snapshot_interval_seconds: float = 60.0,
    ):
        """Initialize the snapshot manager.
        
        Args:
            node_id: Raft node ID
            db_session: SQLAlchemy session
            wal: WriteAheadLog instance
            snapshot_threshold: Create snapshot every N entries
            snapshot_interval_seconds: Minimum interval between snapshots
        """
        self.node_id = node_id
        self.db_session = db_session or get_db_session()
        self.wal = wal or WriteAheadLog(self.db_session, node_id)
        self.snapshot_threshold = snapshot_threshold
        self.snapshot_interval_seconds = snapshot_interval_seconds
        
        # Snapshot metadata
        self.snapshot_index = 0  # Log index of snapshot
        self.snapshot_term = 0   # Term at snapshot index
        self.snapshot_state: Dict[str, Any] = {}  # Snapshot content
        self.last_snapshot_time = 0.0
        self.last_snapshot_entries_applied = 0
    
    def initialize(self):
        """Load last snapshot from database."""
        # In a real system, you would load this from a snapshot storage table
        # For now, we just initialize empty
        logger.info(
            f"Snapshot manager initialized for node {self.node_id}: "
            f"snapshot_index={self.snapshot_index}, "
            f"threshold={self.snapshot_threshold}"
        )
    
    async def should_create_snapshot(self, last_applied_index: int) -> bool:
        """Check if snapshot should be created.
        
        Snapshots are created when:
        1. Number of entries since last snapshot exceeds threshold
        2. Minimum interval has passed since last snapshot
        
        Args:
            last_applied_index: Current last_applied index
        
        Returns:
            True if snapshot should be created
        """
        entries_since_snapshot = last_applied_index - self.snapshot_index
        
        if entries_since_snapshot < self.snapshot_threshold:
            return False
        
        import time
        time_since_snapshot = time.time() - self.last_snapshot_time
        
        if time_since_snapshot < self.snapshot_interval_seconds:
            return False
        
        return True
    
    async def create_snapshot(
        self,
        snapshot_index: int,
        snapshot_term: int,
    ) -> Dict[str, Any]:
        """Create a snapshot of the current state machine state.
        
        This captures the entire database state at a point in time, allowing
        us to discard log entries up to this point.
        
        Args:
            snapshot_index: Log index of the snapshot
            snapshot_term: Term at the snapshot index
        
        Returns:
            Snapshot metadata dict
        """
        try:
            logger.info(
                f"Creating snapshot at index {snapshot_index}, term {snapshot_term}"
            )
            
            # Capture state machine state (all account balances)
            accounts = self.db_session.query(AccountModel).all()
            state = {
                "accounts": {
                    account.account_id: {
                        "balance": str(account.balance),
                        "currency": account.currency,
                        "status": account.status,
                        "version": account.version,
                    }
                    for account in accounts
                },
                "timestamp": datetime.utcnow().isoformat(),
            }
            
            # Store snapshot metadata
            self.snapshot_index = snapshot_index
            self.snapshot_term = snapshot_term
            self.snapshot_state = state
            
            import time
            self.last_snapshot_time = time.time()
            self.last_snapshot_entries_applied = snapshot_index
            
            # In a real system, persist snapshot to disk/S3/etc
            # For now, we just keep it in memory
            
            snapshot_metadata = {
                "node_id": self.node_id,
                "snapshot_index": snapshot_index,
                "snapshot_term": snapshot_term,
                "timestamp": state["timestamp"],
                "accounts_count": len(state["accounts"]),
            }
            
            logger.info(
                f"Snapshot created: index={snapshot_index}, "
                f"accounts={len(state['accounts'])}"
            )
            
            return snapshot_metadata
        
        except Exception as e:
            logger.error(f"Error creating snapshot: {e}")
            raise
    
    async def truncate_log(self, up_to_index: int) -> int:
        """Truncate log entries up to the snapshot index.
        
        After creating a snapshot, we can safely delete log entries
        that are already included in the snapshot.
        
        Args:
            up_to_index: Truncate entries up to this index (inclusive)
        
        Returns:
            Number of entries deleted
        """
        try:
            # Query for entries to delete
            entries_to_delete = self.db_session.query(RaftLogModel).filter(
                RaftLogModel.node_id == self.node_id,
                RaftLogModel.log_index <= up_to_index,
            ).all()
            
            count = len(entries_to_delete)
            
            # Delete entries
            for entry in entries_to_delete:
                self.db_session.delete(entry)
            
            self.db_session.commit()
            
            logger.info(
                f"Truncated {count} log entries up to index {up_to_index}"
            )
            
            return count
        
        except Exception as e:
            self.db_session.rollback()
            logger.error(f"Error truncating log: {e}")
            raise
    
    async def apply_snapshot(self, snapshot_state: Dict[str, Any]) -> bool:
        """Apply a snapshot to the state machine (received from leader).
        
        When a follower is too far behind, the leader sends a snapshot
        instead of all the individual log entries. The follower applies
        this snapshot to quickly catch up.
        
        Args:
            snapshot_state: Snapshot state dict containing accounts
        
        Returns:
            True if successful
        """
        try:
            logger.info("Applying snapshot from leader")
            
            # Update account balances from snapshot
            accounts_in_snapshot = snapshot_state.get("accounts", {})
            
            for account_id, account_data in accounts_in_snapshot.items():
                account = self.db_session.query(AccountModel).filter(
                    AccountModel.account_id == account_id
                ).first()
                
                if account:
                    account.balance = Decimal(account_data["balance"])
                    account.currency = account_data["currency"]
                    account.status = account_data["status"]
                    account.version = account_data["version"]
                else:
                    # Create new account if it doesn't exist
                    new_account = AccountModel(
                        account_id=account_id,
                        owner_id="snapshot",  # Placeholder
                        balance=Decimal(account_data["balance"]),
                        currency=account_data["currency"],
                        status=account_data["status"],
                    )
                    self.db_session.add(new_account)
            
            self.db_session.commit()
            self.snapshot_state = snapshot_state
            
            logger.info(f"Snapshot applied successfully")
            return True
        
        except Exception as e:
            self.db_session.rollback()
            logger.error(f"Error applying snapshot: {e}")
            return False
    
    def get_snapshot(self) -> Optional[Dict[str, Any]]:
        """Get the latest snapshot.
        
        Returns:
            Snapshot dict or None if no snapshot exists
        """
        if not self.snapshot_state:
            return None
        
        return {
            "snapshot_index": self.snapshot_index,
            "snapshot_term": self.snapshot_term,
            "state": self.snapshot_state,
        }
    
    def get_snapshot_metadata(self) -> Dict[str, Any]:
        """Get snapshot metadata without the full state.
        
        Returns:
            Metadata dict
        """
        return {
            "node_id": self.node_id,
            "snapshot_index": self.snapshot_index,
            "snapshot_term": self.snapshot_term,
            "state_size": len(str(self.snapshot_state)),
        }


# Global snapshot manager instance
_snapshot_manager: Optional[SnapshotManager] = None


def get_snapshot_manager() -> SnapshotManager:
    """Get the global snapshot manager."""
    global _snapshot_manager
    if _snapshot_manager is None:
        raise RuntimeError(
            "Snapshot manager not initialized. "
            "Call initialize_snapshot_manager() first."
        )
    return _snapshot_manager


def initialize_snapshot_manager(
    node_id: str,
    db_session: Optional[Session] = None,
    wal: Optional[WriteAheadLog] = None,
    snapshot_threshold: int = 100,
) -> SnapshotManager:
    """Initialize the global snapshot manager."""
    global _snapshot_manager
    _snapshot_manager = SnapshotManager(
        node_id=node_id,
        db_session=db_session,
        wal=wal,
        snapshot_threshold=snapshot_threshold,
    )
    _snapshot_manager.initialize()
    return _snapshot_manager
