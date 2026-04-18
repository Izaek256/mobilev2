"""Write-Ahead Log (WAL) for Raft consensus - persists log entries to database."""

import json
import logging
from typing import List, Optional
from datetime import datetime
from uuid import uuid4
from sqlalchemy.orm import Session
from sqlalchemy import and_

from src.infrastructure.adapter.persistence.sqlalchemy_models import RaftLogModel

logger = logging.getLogger(__name__)


class WriteAheadLog:
    """Write-Ahead Log implementation for Raft consensus."""
    
    def __init__(self, session: Session, node_id: str):
        self.session = session
        self.node_id = node_id
    
    def append_entry(self, term: int, index: int, data: dict, entry_id: str = None) -> str:
        """Append a log entry to the WAL.
        
        Args:
            term: The current term
            index: The log index
            data: The entry data
            entry_id: Optional entry ID (defaults to generated UUID)
            
        Returns:
            The entry ID
        """
        if not entry_id:
            entry_id = str(uuid4())
        
        payload = json.dumps(data)
        
        model = RaftLogModel(
            node_id=self.node_id,
            term=term,
            log_index=index,
            entry_id=entry_id,
            payload=payload,
            applied=False,
            timestamp=datetime.utcnow()
        )
        
        self.session.add(model)
        self.session.flush()
        
        logger.debug(f"WAL: Appended entry {entry_id} at index {index} in term {term}")
        return entry_id
    
    def get_entries(self, from_index: int = 1, to_index: Optional[int] = None) -> List[dict]:
        """Get log entries from the WAL in a range.
        
        Args:
            from_index: Starting log index (inclusive)
            to_index: Ending log index (inclusive), None for all
            
        Returns:
            List of log entries
        """
        query = self.session.query(RaftLogModel).filter(
            and_(
                RaftLogModel.node_id == self.node_id,
                RaftLogModel.log_index >= from_index
            )
        )
        
        if to_index:
            query = query.filter(RaftLogModel.log_index <= to_index)
        
        models = query.order_by(RaftLogModel.log_index).all()
        
        entries = []
        for model in models:
            entry = {
                "term": model.term,
                "index": model.log_index,
                "entry_id": model.entry_id,
                "data": json.loads(model.payload),
                "timestamp": model.timestamp.isoformat(),
                "applied": model.applied
            }
            entries.append(entry)
        
        return entries
    
    def get_entry(self, index: int) -> Optional[dict]:
        """Get a specific log entry by index.
        
        Args:
            index: The log index
            
        Returns:
            The log entry or None if not found
        """
        model = self.session.query(RaftLogModel).filter(
            and_(
                RaftLogModel.node_id == self.node_id,
                RaftLogModel.log_index == index
            )
        ).first()
        
        if not model:
            return None
        
        return {
            "term": model.term,
            "index": model.log_index,
            "entry_id": model.entry_id,
            "data": json.loads(model.payload),
            "timestamp": model.timestamp.isoformat(),
            "applied": model.applied
        }
    
    def mark_applied(self, index: int) -> None:
        """Mark a log entry as applied.
        
        Args:
            index: The log index
        """
        model = self.session.query(RaftLogModel).filter(
            and_(
                RaftLogModel.node_id == self.node_id,
                RaftLogModel.log_index == index
            )
        ).first()
        
        if model:
            model.applied = True
            model.applied_at = datetime.utcnow()
            self.session.flush()
            logger.debug(f"WAL: Marked entry at index {index} as applied")
    
    def mark_range_applied(self, from_index: int, to_index: int) -> None:
        """Mark a range of log entries as applied.
        
        Args:
            from_index: Starting log index (inclusive)
            to_index: Ending log index (inclusive)
        """
        models = self.session.query(RaftLogModel).filter(
            and_(
                RaftLogModel.node_id == self.node_id,
                RaftLogModel.log_index >= from_index,
                RaftLogModel.log_index <= to_index
            )
        ).all()
        
        for model in models:
            model.applied = True
            model.applied_at = datetime.utcnow()
        
        self.session.flush()
        logger.debug(f"WAL: Marked entries {from_index}-{to_index} as applied")
    
    def get_last_index(self) -> int:
        """Get the index of the last log entry.
        
        Returns:
            The last log index, or 0 if log is empty
        """
        model = self.session.query(RaftLogModel).filter(
            RaftLogModel.node_id == self.node_id
        ).order_by(RaftLogModel.log_index.desc()).first()
        
        return model.log_index if model else 0
    
    def get_last_term(self) -> int:
        """Get the term of the last log entry.
        
        Returns:
            The term of the last entry, or 0 if log is empty
        """
        model = self.session.query(RaftLogModel).filter(
            RaftLogModel.node_id == self.node_id
        ).order_by(RaftLogModel.log_index.desc()).first()
        
        return model.term if model else 0
    
    def get_term_at_index(self, index: int) -> int:
        """Get the term of the entry at a specific index.
        
        Args:
            index: The log index
            
        Returns:
            The term, or 0 if entry not found
        """
        if index == 0:
            return 0
        
        model = self.session.query(RaftLogModel).filter(
            and_(
                RaftLogModel.node_id == self.node_id,
                RaftLogModel.log_index == index
            )
        ).first()
        
        return model.term if model else 0
    
    def delete_from_index(self, index: int) -> int:
        """Delete all entries from a given index onwards (for conflict resolution).
        
        Args:
            index: The starting log index to delete from
            
        Returns:
            Number of entries deleted
        """
        models = self.session.query(RaftLogModel).filter(
            and_(
                RaftLogModel.node_id == self.node_id,
                RaftLogModel.log_index >= index
            )
        ).all()
        
        count = len(models)
        for model in models:
            self.session.delete(model)
        
        self.session.flush()
        logger.debug(f"WAL: Deleted {count} entries from index {index}")
        return count
    
    def get_unapplied_entries(self, from_index: int = 1) -> List[dict]:
        """Get all unapplied entries from a given index.
        
        Args:
            from_index: Starting log index (inclusive)
            
        Returns:
            List of unapplied log entries
        """
        models = self.session.query(RaftLogModel).filter(
            and_(
                RaftLogModel.node_id == self.node_id,
                RaftLogModel.log_index >= from_index,
                RaftLogModel.applied == False
            )
        ).order_by(RaftLogModel.log_index).all()
        
        entries = []
        for model in models:
            entry = {
                "term": model.term,
                "index": model.log_index,
                "entry_id": model.entry_id,
                "data": json.loads(model.payload),
                "timestamp": model.timestamp.isoformat()
            }
            entries.append(entry)
        
        return entries
