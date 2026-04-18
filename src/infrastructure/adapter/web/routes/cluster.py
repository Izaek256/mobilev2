"""Cluster and Raft endpoints."""

from fastapi import APIRouter, Request, HTTPException, Depends
from pydantic import BaseModel
from typing import List, Any, Optional
import logging
from datetime import datetime

from src.infrastructure.config.database import get_db_session
import src.infrastructure.consensus.raft_node as raft_module
from src.infrastructure.config.cluster_state import ClusterStateManager

router = APIRouter(prefix="/raft", tags=["raft"])
logger = logging.getLogger(__name__)


class RequestVoteRequest(BaseModel):
    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int


class AppendEntriesRequest(BaseModel):
    term: int
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    entries: List[Any]
    leader_commit: int


class NodeStatusResponse(BaseModel):
    node_id: str
    state: str
    term: int
    commit_index: int
    last_applied: int
    log_length: int
    leader_id: Optional[str]
    timestamp: str


class ClusterStatusResponse(BaseModel):
    leader_id: Optional[str]
    term: int
    nodes: List[dict]
    quorum_size: int
    timestamp: str


class NodeRegistrationRequest(BaseModel):
    node_id: str
    address: str


@router.post("/request_vote")
async def request_vote(req: RequestVoteRequest):
    """Handle RequestVote RPC from candidate."""
    if raft_module.node is None:
        raise HTTPException(status_code=503, detail="Raft node not initialized")
    return await raft_module.node.handle_request_vote(
        req.term, req.candidate_id, req.last_log_index, req.last_log_term
    )


@router.post("/append_entries")
async def append_entries(req: AppendEntriesRequest):
    """Handle AppendEntries RPC from leader."""
    if raft_module.node is None:
        raise HTTPException(status_code=503, detail="Raft node not initialized")
    return await raft_module.node.handle_append_entries(
        req.term, req.leader_id, req.prev_log_index, req.prev_log_term, req.entries, req.leader_commit
    )


@router.get("/status", response_model=NodeStatusResponse)
async def node_status():
    """Get the status of this node."""
    if raft_node is None:
        return {
            "node_id": "unknown",
            "state": "uninitialized",
            "term": 0,
            "commit_index": 0,
            "last_applied": 0,
            "log_length": 0,
            "leader_id": None,
            "timestamp": datetime.utcnow().isoformat()
        }
    
    status = raft_node.get_status()
    return status


@router.get("/cluster/status", response_model=ClusterStatusResponse)
async def cluster_status(db=Depends(get_db_session)):
    """Get the status of the entire cluster."""
    if raft_node is None:
        raise HTTPException(status_code=503, detail="Raft node not initialized")
    
    cluster_mgr = ClusterStateManager(db, raft_node.node_id)
    all_nodes = cluster_mgr.get_all_nodes()
    leader = cluster_mgr.get_leader_node()
    
    return {
        "leader_id": leader["node_id"] if leader else None,
        "term": raft_node.current_term,
        "nodes": all_nodes,
        "quorum_size": cluster_mgr.get_quorum_size(),
        "timestamp": datetime.utcnow().isoformat()
    }


@router.get("/cluster/nodes")
async def list_cluster_nodes(db=Depends(get_db_session)):
    """List all nodes in the cluster."""
    if raft_node is None:
        raise HTTPException(status_code=503, detail="Raft node not initialized")
    
    cluster_mgr = ClusterStateManager(db, raft_node.node_id)
    return {"nodes": cluster_mgr.get_all_nodes()}


@router.post("/cluster/register")
async def register_node(req: NodeRegistrationRequest, db=Depends(get_db_session)):
    """Register a new node in the cluster."""
    if raft_node is None:
        raise HTTPException(status_code=503, detail="Raft node not initialized")
    
    cluster_mgr = ClusterStateManager(db, raft_node.node_id)
    cluster_mgr.register_node(req.node_id, req.address)
    db.commit()
    
    return {"status": "registered", "node_id": req.node_id}


@router.get("/cluster/leader")
async def get_leader(db=Depends(get_db_session)):
    """Get the current leader of the cluster."""
    if raft_module.node is None:
        raise HTTPException(status_code=503, detail="Raft node not initialized")
    
    if raft_module.node.is_leader():
        return {
            "leader_id": raft_module.node.node_id,
            "address": "http://localhost:8000",  # Could be configured
            "is_current_node": True
        }
    
    if raft_module.node.get_leader_id():
        cluster_mgr = ClusterStateManager(db, raft_module.node.node_id)
        leader = cluster_mgr.get_node(raft_module.node.get_leader_id())
        if leader:
            return {
                "leader_id": leader["node_id"],
                "address": leader["address"],
                "is_current_node": False
            }
    
    raise HTTPException(status_code=503, detail="No leader elected")


@router.get("/cluster/health")
async def cluster_health(db=Depends(get_db_session)):
    """Get the health status of the cluster."""
    if raft_module.node is None:
        raise HTTPException(status_code=503, detail="Raft node not initialized")
    
    cluster_mgr = ClusterStateManager(db, raft_module.node.node_id)
    cluster_mgr.check_node_health()
    db.commit()
    
    all_nodes = cluster_mgr.get_all_nodes()
    healthy_nodes = cluster_mgr.get_healthy_nodes()
    quorum_size = cluster_mgr.get_quorum_size()
    
    health_status = {
        "healthy": len(healthy_nodes) >= quorum_size,
        "total_nodes": len(all_nodes),
        "healthy_nodes": len(healthy_nodes),
        "quorum_size": quorum_size,
        "nodes": all_nodes
    }
    
    return health_status

