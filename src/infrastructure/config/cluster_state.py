"""Cluster State Management - tracks peer nodes and their metadata."""

import logging
from typing import Dict, List, Optional
from datetime import datetime, timedelta
from sqlalchemy.orm import Session

from src.infrastructure.adapter.persistence.sqlalchemy_models import ClusterStateModel, IdempotencyKeyModel

logger = logging.getLogger(__name__)


class ClusterStateManager:
    """Manages cluster state including peer nodes and their health status."""
    
    def __init__(self, session: Session, node_id: str):
        self.session = session
        self.node_id = node_id
        self.health_check_timeout = 5  # seconds
    
    def register_node(self, node_id: str, address: str) -> None:
        """Register or update a node in the cluster.
        
        Args:
            node_id: The node ID
            address: The node address (e.g., "http://node-2:8000")
        """
        existing = self.session.query(ClusterStateModel).filter(
            ClusterStateModel.node_id == node_id
        ).first()
        
        if existing:
            existing.address = address
            existing.updated_at = datetime.utcnow()
        else:
            model = ClusterStateModel(
                node_id=node_id,
                address=address,
                role="follower",
                status="unknown"
            )
            self.session.add(model)
        
        self.session.flush()
        logger.debug(f"Registered node {node_id} at {address}")
    
    def update_node_role(self, node_id: str, role: str) -> None:
        """Update the role of a node.
        
        Args:
            node_id: The node ID
            role: The role (leader, follower, candidate)
        """
        model = self.session.query(ClusterStateModel).filter(
            ClusterStateModel.node_id == node_id
        ).first()
        
        if model:
            model.role = role
            model.updated_at = datetime.utcnow()
            self.session.flush()
            logger.debug(f"Updated node {node_id} role to {role}")
    
    def update_node_term(self, node_id: str, term: int) -> None:
        """Update the term of a node.
        
        Args:
            node_id: The node ID
            term: The term number
        """
        model = self.session.query(ClusterStateModel).filter(
            ClusterStateModel.node_id == node_id
        ).first()
        
        if model:
            model.term = term
            model.updated_at = datetime.utcnow()
            self.session.flush()
    
    def update_node_heartbeat(self, node_id: str) -> None:
        """Update the last heartbeat timestamp for a node.
        
        Args:
            node_id: The node ID
        """
        model = self.session.query(ClusterStateModel).filter(
            ClusterStateModel.node_id == node_id
        ).first()
        
        if model:
            model.last_heartbeat = datetime.utcnow()
            model.status = "healthy"
            model.updated_at = datetime.utcnow()
            self.session.flush()
    
    def mark_node_unhealthy(self, node_id: str) -> None:
        """Mark a node as unhealthy.
        
        Args:
            node_id: The node ID
        """
        model = self.session.query(ClusterStateModel).filter(
            ClusterStateModel.node_id == node_id
        ).first()
        
        if model:
            model.status = "unhealthy"
            model.updated_at = datetime.utcnow()
            self.session.flush()
            logger.warning(f"Marked node {node_id} as unhealthy")
    
    def get_node(self, node_id: str) -> Optional[Dict]:
        """Get node metadata.
        
        Args:
            node_id: The node ID
            
        Returns:
            Node metadata or None if not found
        """
        model = self.session.query(ClusterStateModel).filter(
            ClusterStateModel.node_id == node_id
        ).first()
        
        if not model:
            return None
        
        return {
            "node_id": model.node_id,
            "address": model.address,
            "role": model.role,
            "term": model.term,
            "status": model.status,
            "last_heartbeat": model.last_heartbeat.isoformat() if model.last_heartbeat else None,
            "updated_at": model.updated_at.isoformat()
        }
    
    def get_all_nodes(self) -> List[Dict]:
        """Get all nodes in the cluster.
        
        Returns:
            List of node metadata
        """
        models = self.session.query(ClusterStateModel).order_by(ClusterStateModel.node_id).all()
        
        nodes = []
        for model in models:
            node = {
                "node_id": model.node_id,
                "address": model.address,
                "role": model.role,
                "term": model.term,
                "status": model.status,
                "last_heartbeat": model.last_heartbeat.isoformat() if model.last_heartbeat else None,
                "updated_at": model.updated_at.isoformat()
            }
            nodes.append(node)
        
        return nodes
    
    def get_healthy_nodes(self) -> List[Dict]:
        """Get all healthy nodes in the cluster.
        
        Returns:
            List of healthy node metadata
        """
        nodes = self.get_all_nodes()
        return [n for n in nodes if n["status"] == "healthy"]
    
    def get_leader_node(self) -> Optional[Dict]:
        """Get the leader node.
        
        Returns:
            Leader node metadata or None if not found
        """
        model = self.session.query(ClusterStateModel).filter(
            ClusterStateModel.role == "leader"
        ).first()
        
        if not model:
            return None
        
        return {
            "node_id": model.node_id,
            "address": model.address,
            "role": model.role,
            "term": model.term,
            "status": model.status,
            "last_heartbeat": model.last_heartbeat.isoformat() if model.last_heartbeat else None,
            "updated_at": model.updated_at.isoformat()
        }
    
    def check_node_health(self) -> None:
        """Check health of all nodes based on heartbeat timeout."""
        now = datetime.utcnow()
        timeout = timedelta(seconds=self.health_check_timeout)
        
        models = self.session.query(ClusterStateModel).all()
        for model in models:
            if model.node_id == self.node_id:
                continue
            
            if model.last_heartbeat and (now - model.last_heartbeat) > timeout:
                if model.status != "unhealthy":
                    logger.warning(f"Node {model.node_id} marked unhealthy due to heartbeat timeout")
                    model.status = "unhealthy"
                    self.session.flush()
    
    def get_quorum_size(self) -> int:
        """Get the quorum size for the cluster.
        
        Returns:
            Number of nodes needed for quorum
        """
        nodes = self.get_all_nodes()
        return len(nodes) // 2 + 1


class IdempotencyManager:
    """Manages idempotency keys across the cluster to prevent duplicate transactions."""
    
    def __init__(self, session: Session):
        self.session = session
    
    def check_idempotency_key(self, idempotency_key: str) -> Optional[Dict]:
        """Check if an idempotency key has been processed.
        
        Args:
            idempotency_key: The idempotency key
            
        Returns:
            Idempotency record or None if not found
        """
        model = self.session.query(IdempotencyKeyModel).filter(
            IdempotencyKeyModel.idempotency_key == idempotency_key
        ).first()
        
        if not model:
            return None
        
        return {
            "idempotency_key": model.idempotency_key,
            "transaction_id": model.transaction_id,
            "status": model.status,
            "result": model.result,
            "node_id": model.node_id,
            "created_at": model.created_at.isoformat(),
            "completed_at": model.completed_at.isoformat() if model.completed_at else None
        }
    
    def register_idempotency_key(self, idempotency_key: str, transaction_id: str, node_id: str) -> None:
        """Register a new idempotency key.
        
        Args:
            idempotency_key: The idempotency key
            transaction_id: The transaction ID
            node_id: The node that initiated the transaction
        """
        # Check if already exists
        existing = self.session.query(IdempotencyKeyModel).filter(
            IdempotencyKeyModel.idempotency_key == idempotency_key
        ).first()
        
        if existing:
            raise ValueError(f"Idempotency key {idempotency_key} already exists")
        
        model = IdempotencyKeyModel(
            idempotency_key=idempotency_key,
            transaction_id=transaction_id,
            status="pending",
            node_id=node_id
        )
        self.session.add(model)
        self.session.flush()
        logger.debug(f"Registered idempotency key {idempotency_key}")
    
    def mark_completed(self, idempotency_key: str, result: str = None) -> None:
        """Mark an idempotency key as completed.
        
        Args:
            idempotency_key: The idempotency key
            result: Optional result JSON
        """
        model = self.session.query(IdempotencyKeyModel).filter(
            IdempotencyKeyModel.idempotency_key == idempotency_key
        ).first()
        
        if model:
            model.status = "completed"
            model.result = result
            model.completed_at = datetime.utcnow()
            self.session.flush()
            logger.debug(f"Marked idempotency key {idempotency_key} as completed")
    
    def mark_failed(self, idempotency_key: str, result: str = None) -> None:
        """Mark an idempotency key as failed.
        
        Args:
            idempotency_key: The idempotency key
            result: Optional error message
        """
        model = self.session.query(IdempotencyKeyModel).filter(
            IdempotencyKeyModel.idempotency_key == idempotency_key
        ).first()
        
        if model:
            model.status = "failed"
            model.result = result
            model.completed_at = datetime.utcnow()
            self.session.flush()
            logger.debug(f"Marked idempotency key {idempotency_key} as failed")
