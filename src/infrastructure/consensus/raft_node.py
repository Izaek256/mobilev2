"""Raft Consensus Node Implementation."""

import asyncio
import httpx
import logging
import random
import json
from enum import Enum
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
from collections import defaultdict

logger = logging.getLogger(__name__)

class NodeState(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"

class LogEntry:
    """Represents a log entry in Raft."""
    
    def __init__(self, term: int, index: int, data: Dict[str, Any], entry_id: str = None):
        self.term = term
        self.index = index
        self.data = data
        self.entry_id = entry_id or f"{term}-{index}"
        self.timestamp = datetime.utcnow()
        
    def to_dict(self) -> Dict[str, Any]:
        return {
            "term": self.term,
            "index": self.index,
            "data": self.data,
            "entry_id": self.entry_id,
            "timestamp": self.timestamp.isoformat()
        }
    
    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "LogEntry":
        entry = LogEntry(d["term"], d["index"], d["data"], d.get("entry_id"))
        entry.timestamp = datetime.fromisoformat(d["timestamp"])
        return entry

class RaftNode:
    """Full Raft Consensus Node Implementation."""
    
    def __init__(self, node_id: str, peers: List[str]):
        self.node_id = node_id
        self.peers = [p for p in peers if p]  # Filter empty peers
        self.state = NodeState.FOLLOWER
        self.current_term = 0
        self.voted_for = None
        self.log: List[LogEntry] = []  # Persistent log
        self.commit_index = 0
        self.last_applied = 0
        self.leader_id = None
        
        # Follower state tracking (for leader only)
        self.next_index: Dict[str, int] = {p: 1 for p in self.peers}
        self.match_index: Dict[str, int] = {p: 0 for p in self.peers}
        
        # Replication tracking
        self.replication_pending: Dict[str, asyncio.Event] = {}
        self.last_heartbeat_time = datetime.utcnow()
        
        # Ready event - signals when cluster is ready for requests
        # Set when: (1) this node becomes leader, or (2) this node receives heartbeat from leader
        self.ready_event = asyncio.Event()
        
        self.election_timeout_task = None
        self.heartbeat_task = None
        self.apply_task = None
        
        # httpx client for async HTTP requests
        self.client = httpx.AsyncClient(timeout=2.0)
        
    async def start(self):
        """Start the Raft node."""
        logger.info(f"Starting Raft node {self.node_id}")
        logger.info(f"Node peers: {self.peers}")
        
        if not self.peers:
            # Single node cluster, become leader immediately
            self.state = NodeState.LEADER
            self.current_term = 1
            self.leader_id = self.node_id
            logger.info(f"Single node cluster: {self.node_id} became LEADER")
            self.ready_event.set()  # Signal ready immediately for single-node cluster
        else:
            # Give the server time to fully start before beginning election
            await asyncio.sleep(2)
            logger.info(f"Node {self.node_id} starting election process")
            self._reset_election_timeout()
        
        # Start the apply state machine task
        self.apply_task = asyncio.create_task(self._apply_loop())
    
    def get_log_term(self, index: int) -> int:
        """Get the term of a log entry at the given index."""
        if index == 0:
            return 0
        if 1 <= index <= len(self.log):
            return self.log[index - 1].term
        return 0
    
    def get_last_log_index(self) -> int:
        """Get the index of the last log entry."""
        return len(self.log)
    
    def get_last_log_term(self) -> int:
        """Get the term of the last log entry."""
        return self.get_log_term(self.get_last_log_index())

    def _reset_election_timeout(self):
        """Reset election timeout. Called when we receive RPC from leader or vote."""
        if self.election_timeout_task:
            self.election_timeout_task.cancel()
        timeout = random.uniform(1.5, 3.0)  # Seconds
        self.election_timeout_task = asyncio.create_task(self._election_timer(timeout))
        
    async def _election_timer(self, timeout: float):
        """Election timeout timer."""
        try:
            await asyncio.sleep(timeout)
            await self._start_election()
        except asyncio.CancelledError:
            pass

    async def _start_election(self):
        """Start a new election."""
        self.state = NodeState.CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        votes = 1
        logger.info(f"Node {self.node_id} starting election for term {self.current_term}")
        
        self._reset_election_timeout()
        
        if not self.peers:
            # Single node, become leader
            self.state = NodeState.LEADER
            self.leader_id = self.node_id
            logger.info(f"Node {self.node_id} became LEADER (single node)")
            self._start_heartbeats()
            return
        
        tasks = []
        for peer in self.peers:
            tasks.append(self._request_vote(peer))
            
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, dict) and result.get("vote_granted"):
                votes += 1
                
        quorum_size = len(self.peers) // 2 + 1
        if votes >= quorum_size and self.state == NodeState.CANDIDATE:
            logger.info(f"Node {self.node_id} became LEADER for term {self.current_term} (votes: {votes}/{len(self.peers)+1})")
            self.state = NodeState.LEADER
            self.leader_id = self.node_id
            self.ready_event.set()  # Signal ready when becoming leader
            
            # Initialize leader state
            for peer in self.peers:
                self.next_index[peer] = self.get_last_log_index() + 1
                self.match_index[peer] = 0
            
            if self.election_timeout_task:
                self.election_timeout_task.cancel()
            self._start_heartbeats()
        else:
            logger.warning(f"Node {self.node_id} election failed for term {self.current_term} (votes: {votes}/{len(self.peers)+1})")

    async def _request_vote(self, peer: str) -> Dict[str, Any]:
        """Request vote from a peer."""
        try:
            # Ensure peer URL has http:// prefix
            if not peer.startswith("http://") and not peer.startswith("https://"):
                peer_url = f"http://{peer}"
            else:
                peer_url = peer
                
            payload = {
                "term": self.current_term,
                "candidate_id": self.node_id,
                "last_log_index": self.get_last_log_index(),
                "last_log_term": self.get_last_log_term()
            }
            logger.debug(f"Requesting vote from {peer_url}")
            response = await self.client.post(f"{peer_url}/raft/request_vote", json=payload, timeout=2)
            logger.debug(f"Vote response from {peer}: {response.json()}")
            return response.json()
        except Exception as e:
            logger.warning(f"Error requesting vote from {peer}: {e}")
            return {"term": self.current_term, "vote_granted": False}

    def _start_heartbeats(self):
        """Start sending heartbeats to followers."""
        if self.heartbeat_task:
            self.heartbeat_task.cancel()
        self.heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def _heartbeat_loop(self):
        """Send periodic heartbeats and replicate log to followers."""
        while self.state == NodeState.LEADER:
            try:
                tasks = []
                for peer in self.peers:
                    tasks.append(self._send_append_entries(peer))
                await asyncio.gather(*tasks, return_exceptions=True)
                self.last_heartbeat_time = datetime.utcnow()
            except Exception as e:
                logger.error(f"Error in heartbeat loop: {e}")
            
            await asyncio.sleep(0.3)  # Heartbeat interval

    async def _send_append_entries(self, peer: str):
        """Send AppendEntries RPC to a follower."""
        try:
            # Ensure peer URL has http:// prefix
            if not peer.startswith("http://") and not peer.startswith("https://"):
                peer_url = f"http://{peer}"
            else:
                peer_url = peer
            
            prev_log_index = self.next_index[peer] - 1
            prev_log_term = self.get_log_term(prev_log_index)
            
            # Collect entries to send
            entries = []
            for i in range(self.next_index[peer], self.get_last_log_index() + 1):
                if 1 <= i <= len(self.log):
                    entries.append(self.log[i - 1].to_dict())
            
            payload = {
                "term": self.current_term,
                "leader_id": self.node_id,
                "prev_log_index": prev_log_index,
                "prev_log_term": prev_log_term,
                "entries": entries,
                "leader_commit": self.commit_index
            }
            
            response = await self.client.post(
                f"{peer_url}/raft/append_entries", 
                json=payload, 
                timeout=2
            )
            data = response.json()
            
            # Handle response
            if data.get("term", 0) > self.current_term:
                # Higher term, step down
                self.current_term = data["term"]
                self.state = NodeState.FOLLOWER
                self.voted_for = None
                self.leader_id = None
                self._reset_election_timeout()
                return
            
            if data.get("success"):
                # Replication successful
                new_match_index = prev_log_index + len(entries)
                self.match_index[peer] = new_match_index
                self.next_index[peer] = new_match_index + 1
                
                # Check if we can advance commit_index
                self._update_commit_index()
            else:
                # Replication failed, backtrack
                if self.next_index[peer] > 1:
                    self.next_index[peer] -= 1
                    
        except Exception as e:
            logger.debug(f"Error sending append_entries to {peer}: {e}")

    def _update_commit_index(self):
        """Update commit index based on replication progress."""
        if self.state != NodeState.LEADER:
            return
        
        # Find the highest index that is replicated on a majority
        match_indices = [self.match_index[p] for p in self.peers] + [self.get_last_log_index()]
        match_indices.sort(reverse=True)
        quorum_size = len(self.peers) // 2 + 1
        
        if len(match_indices) >= quorum_size:
            new_commit_index = match_indices[quorum_size - 1]
            
            # Only commit entries from current term
            if new_commit_index > self.commit_index and self.get_log_term(new_commit_index) == self.current_term:
                old_commit = self.commit_index
                self.commit_index = new_commit_index
                logger.debug(f"Advanced commit_index from {old_commit} to {self.commit_index}")
    
    async def _apply_loop(self):
        """Apply committed entries to state machine."""
        while True:
            try:
                while self.last_applied < self.commit_index:
                    self.last_applied += 1
                    if 1 <= self.last_applied <= len(self.log):
                        entry = self.log[self.last_applied - 1]
                        await self._apply_entry(entry)
                await asyncio.sleep(0.1)
            except Exception as e:
                logger.error(f"Error in apply loop: {e}")
                await asyncio.sleep(0.1)
    
    async def _apply_entry(self, entry: LogEntry):
        """
        Apply a log entry to the state machine.
        
        This is called when an entry is committed (replicated to majority).
        The state machine applier will check idempotency and apply deterministically.
        """
        logger.info(f"Applying committed entry: {entry.entry_id} at index {entry.index}")
        # The state machine applier will be called from the consensus transaction manager
        # This hook is here for future integration with persistent state machine application
    
    def append_entry(self, data: Dict[str, Any]) -> int:
        """
        Append an entry to the Raft log (leader only).
        
        CONSENSUS-FIRST: This is the critical method that bridges API and Raft.
        
        Args:
            data: Entry data (contains transaction_id, operation_type, payload)
            
        Returns:
            Index of the appended entry
            
        Raises:
            RuntimeError: If not the leader
        """
        if self.state != NodeState.LEADER:
            raise RuntimeError(f"Not leader. Current state: {self.state}")
        
        # Create log entry with current term
        next_index = self.get_last_log_index() + 1
        entry = LogEntry(self.current_term, next_index, data)
        self.log.append(entry)
        
        logger.info(
            f"Leader {self.node_id} appended entry at index {next_index}: "
            f"transaction_id={data.get('transaction_id')}"
        )
        
        # For single-node clusters, immediately update commit_index
        # Since the leader itself forms a quorum, entries are committed instantly
        if not self.peers:
            self.commit_index = next_index
            logger.info(f"Single-node cluster: committed entry at index {next_index}")
        else:
            # For multi-node clusters, update commit_index based on replication progress
            self._update_commit_index()
        
        return next_index
    
    async def handle_request_vote(self, term: int, candidate_id: str, last_log_index: int, last_log_term: int) -> Dict[str, Any]:
        """Handle RequestVote RPC."""
        # If term is greater, update our term and step down
        if term > self.current_term:
            self.current_term = term
            self.state = NodeState.FOLLOWER
            self.voted_for = None
            self.leader_id = None
            
        # Check if we can vote for this candidate
        vote_granted = False
        if term == self.current_term:
            if self.voted_for is None or self.voted_for == candidate_id:
                # Check log consistency
                if last_log_term > self.get_last_log_term() or \
                   (last_log_term == self.get_last_log_term() and last_log_index >= self.get_last_log_index()):
                    vote_granted = True
                    self.voted_for = candidate_id
                    self._reset_election_timeout()
        
        logger.debug(f"Node {self.node_id} voting {vote_granted} for candidate {candidate_id} in term {term}")
        return {"term": self.current_term, "vote_granted": vote_granted}

    async def handle_append_entries(self, term: int, leader_id: str, prev_log_index: int, prev_log_term: int, entries: List[Dict[str, Any]], leader_commit: int) -> Dict[str, Any]:
        """Handle AppendEntries RPC."""
        # If term is greater, update our term and step down
        if term > self.current_term:
            self.current_term = term
            self.state = NodeState.FOLLOWER
            self.voted_for = None
        
        # Check term
        if term < self.current_term:
            return {"term": self.current_term, "success": False}
        
        # Update leader and reset timeout
        if term >= self.current_term:
            self.state = NodeState.FOLLOWER
            self.leader_id = leader_id
            self.ready_event.set()  # Signal ready when we know the leader
            self._reset_election_timeout()
        
        # Check log consistency
        if prev_log_index > 0 and self.get_log_term(prev_log_index) != prev_log_term:
            logger.warning(f"Log consistency check failed: prev_log_index={prev_log_index}, prev_log_term={prev_log_term}, actual_term={self.get_log_term(prev_log_index)}")
            return {"term": self.current_term, "success": False}
        
        # Append entries
        for entry_dict in entries:
            entry = LogEntry.from_dict(entry_dict)
            index = entry.index
            
            # Extend log if needed
            while len(self.log) < index:
                self.log.append(None)
            
            # Add or replace entry
            if index <= len(self.log):
                self.log[index - 1] = entry
            else:
                self.log.append(entry)
        
        # Update commit index
        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, self.get_last_log_index())
        
        return {"term": self.current_term, "success": True}
    
    async def append_log(self, event_data: dict) -> Tuple[bool, Optional[str]]:
        """
        Append an event to the log (must be called on leader).
        Returns (success, error_message)
        """
        if self.state != NodeState.LEADER:
            # Followers can't accept writes
            return False, f"Not leader. Current leader: {self.leader_id}"
        
        entry_id = f"{self.current_term}-{self.get_last_log_index() + 1}"
        entry = LogEntry(self.current_term, self.get_last_log_index() + 1, event_data, entry_id)
        self.log.append(entry)
        
        logger.info(f"Leader {self.node_id} appended log entry: {entry_id}")
        
        # If single node, commit immediately
        if not self.peers:
            self.commit_index = self.get_last_log_index()
            return True, None
        
        # Wait for replication to a majority (with timeout)
        try:
            # Create an event to signal replication completion
            event = asyncio.Event()
            self.replication_pending[entry_id] = event
            
            # Wait for replication (with 5 second timeout)
            await asyncio.wait_for(event.wait(), timeout=5.0)
            return True, None
        except asyncio.TimeoutError:
            return False, "Replication timeout"
        except Exception as e:
            return False, str(e)
        finally:
            self.replication_pending.pop(entry_id, None)
    
    def is_leader(self) -> bool:
        """Check if this node is the leader."""
        return self.state == NodeState.LEADER
    
    def get_leader_id(self) -> Optional[str]:
        """Get the current leader ID."""
        return self.leader_id if self.state == NodeState.FOLLOWER else (self.node_id if self.state == NodeState.LEADER else None)
    
    def get_status(self) -> Dict[str, Any]:
        """Get node status."""
        return {
            "node_id": self.node_id,
            "state": self.state.value,
            "term": self.current_term,
            "voted_for": self.voted_for,
            "commit_index": self.commit_index,
            "last_applied": self.last_applied,
            "log_length": len(self.log),
            "leader_id": self.get_leader_id(),
            "timestamp": datetime.utcnow().isoformat()
        }

# Singleton instance
node = None
