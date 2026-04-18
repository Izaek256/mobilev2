"""FastAPI HTTP adapter setup."""

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError

from src.infrastructure.adapter.web.routes import health, transactions, cluster, accounts
from src.infrastructure.adapter.web.dto import ErrorResponse
import os
import asyncio
import logging
from src.infrastructure.consensus.raft_node import RaftNode
import src.infrastructure.consensus.raft_node as raft_module

logger = logging.getLogger(__name__)


def create_app() -> FastAPI:
    """Create and configure FastAPI application.
    
    Returns:
        Configured FastAPI app
    """
    app = FastAPI(
        title="Distributed Ledger System",
        description="A simple distributed ledger system for money transfers",
        version="1.0.0",
    )
    
    # Include routers
    app.include_router(health.router)
    app.include_router(transactions.router)
    app.include_router(cluster.router)
    app.include_router(accounts.router)
    
    @app.on_event("startup")
    async def startup_event():
        node_id = os.environ.get("NODE_ID", "node-1")
        peers_str = os.environ.get("PEERS", "")
        peers = [p.strip() for p in peers_str.split(",")] if peers_str else []
        
        raft_module.node = RaftNode(node_id, peers)
        # Start the election timeout and apply loop immediately
        # For single-node clusters, this makes the node a leader
        asyncio.create_task(raft_module.node.start())
        
        # Wait for cluster to be ready (leader elected or follower knows leader)
        # Timeout after 10 seconds to avoid blocking server startup forever
        try:
            await asyncio.wait_for(
                raft_module.node.ready_event.wait(),
                timeout=10.0
            )
            logger.info(f"Raft cluster ready. Node state: {raft_module.node.state}, Leader: {raft_module.node.leader_id}")
        except asyncio.TimeoutError:
            logger.warning(
                f"Raft cluster not ready after 10 seconds. "
                f"Node state: {raft_module.node.state}, Leader: {raft_module.node.leader_id}. "
                f"Requests may fail if no leader is elected."
            )
    
    # Exception handlers
    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(request: Request, exc: RequestValidationError):
        """Handle validation errors."""
        errors = []
        for error in exc.errors():
            errors.append({
                "field": ".".join(str(x) for x in error["loc"][1:]),
                "message": error["msg"],
            })
        
        return JSONResponse(
            status_code=400,
            content=ErrorResponse(
                status="error",
                message="Validation error",
                error_code="VALIDATION_ERROR",
            ).model_dump(),
        )
    
    @app.get("/", tags=["root"])
    async def root():
        """Root endpoint."""
        return {"message": "Distributed Ledger System API"}
    
    return app
