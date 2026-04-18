"""
Transaction routes - CONSENSUS-FIRST ARCHITECTURE.

CRITICAL DESIGN PRINCIPLE:
No database writes from API handlers. All state changes come from committed Raft entries.

Flow for every transaction:
1. API receives request with Transaction ID (UUID)
2. Check if current node is Leader (redirect 307 if not)
3. Append to Raft log
4. Wait for commitment (replication to majority)
5. Apply committed entry to state machine (local database)

This ensures:
- Consensus as source of truth
- Idempotent retries (same transaction ID = same result)
- Deterministic state (timestamps from Raft, not system time)
- No partial failures (consensus succeeded = state machine safe)
"""

from fastapi import APIRouter, Depends, Header, HTTPException, status as http_status, Response
from pydantic import BaseModel
from sqlalchemy.orm import Session
import logging
import asyncio
from typing import Optional
from starlette.responses import RedirectResponse

from src.infrastructure.adapter.web.dto import SendMoneyRequest, SendMoneyResponse, ErrorResponse
from src.infrastructure.adapter.web.security import BasicAuthValidator
from src.infrastructure.config.database import get_db_session
from src.infrastructure.consensus.consensus_transaction_manager import (
    ConsensusTransactionManager,
    NotLeaderError,
    ConsensusTimeoutError,
    ConsensusTransactionError,
)
import src.infrastructure.consensus.raft_node as raft_module

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1/transactions", tags=["transactions"])

# Simple auth validator (in production, use proper OAuth2)
auth_validator = BasicAuthValidator(
    valid_username="admin",
    valid_password="password123",
)


class SendMoneyConsensusRequest(BaseModel):
    """Request for consensus-based money transfer."""
    from_account_id: str
    to_account_id: str
    amount: float
    currency: str = "USD"


def verify_auth(authorization: str = Header(None)) -> None:
    """Verify Basic Auth.
    
    Args:
        authorization: Authorization header
        
    Raises:
        HTTPException: If auth is invalid
    """
    auth_validator.validate(authorization)


@router.post("/send", response_model=SendMoneyResponse, responses={
    307: {"description": "Redirect to leader"},
    400: {"model": ErrorResponse},
    401: {"model": ErrorResponse},
    503: {"model": ErrorResponse},
})
async def send_money(
    request: SendMoneyConsensusRequest,
    db: Session = Depends(get_db_session),
    _: None = Depends(verify_auth),
):
    """
    Send money from one account to another via Raft consensus (CONSENSUS-FIRST).
    
    CRITICAL: This endpoint does NOT write directly to the database.
    Instead, it follows the consensus-first workflow:
    
    1. Check if this node is the Leader
    2. If not, return HTTP 307 redirect to leader
    3. If leader, append transaction to Raft log
    4. Wait for commitment (replication to majority)
    5. Apply committed entry to state machine (database)
    
    This ensures:
    - All state changes replicated across cluster
    - Idempotent retries (same transaction = same result)
    - No partial failures (consensus = safety guarantee)
    - Deterministic ordering (Raft total order)
    
    Args:
        request: SendMoneyConsensusRequest with transfer details
        db: Database session (for state machine apply only)
        
    Returns:
        SendMoneyResponse with transaction_id and success status
        
    Raises:
        HTTPException 307: If not leader (redirect to leader)
        HTTPException 503: If consensus fails
        HTTPException 400: If validation fails
    """
    if not raft_module.node:
        raise HTTPException(
            status_code=503,
            detail=ErrorResponse(
                status="error",
                message="Raft consensus not available",
                error_code="CONSENSUS_UNAVAILABLE",
            ).model_dump(),
        )
    
    consensus_mgr = ConsensusTransactionManager(db)
    
    try:
        # STEP 1: Check if current node is the Leader
        if not consensus_mgr.is_leader():
            logger.info(
                f"Request to non-leader node {raft_module.node.node_id}. "
                f"Current state: {raft_module.node.state}. "
                f"Redirecting to leader."
            )
            # STEP 2: Redirect to leader (HTTP 307)
            leader_info = consensus_mgr.get_leader_address()
            if leader_info:
                # In production, maintain node_id -> address mapping
                raise HTTPException(
                    status_code=307,
                    detail=ErrorResponse(
                        status="error",
                        message=f"Redirect to leader {leader_info}",
                        error_code="NOT_LEADER",
                    ).model_dump(),
                )
            else:
                raise HTTPException(
                    status_code=503,
                    detail=ErrorResponse(
                        status="error",
                        message="No leader elected yet. Try again.",
                        error_code="NO_LEADER",
                    ).model_dump(),
                )
        
        # STEP 3: Append to Raft log and STEPS 4-5: Wait for commitment and apply
        logger.info(
            f"Leader processing send_money: {request.from_account_id} -> "
            f"{request.to_account_id}: {request.amount} {request.currency}"
        )
        
        payload = {
            "from_account_id": request.from_account_id,
            "to_account_id": request.to_account_id,
            "amount": request.amount,
            "currency": request.currency,
        }
        
        transaction_id, result = await consensus_mgr.append_and_wait_for_commitment(
            operation_type="send_money",
            payload=payload,
            timeout_seconds=5.0,
        )
        
        logger.info(f"Transaction {transaction_id} completed successfully")
        return SendMoneyResponse(
            status="success",
            message=result.get("result", {}).get("message", "Money transferred successfully"),
            transaction_id=transaction_id,
        )
    
    except NotLeaderError as e:
        logger.info(f"Not leader: {str(e)}")
        raise HTTPException(
            status_code=307,
            detail=ErrorResponse(
                status="error",
                message=str(e),
                error_code="NOT_LEADER",
            ).model_dump(),
        )
    
    except ConsensusTimeoutError as e:
        logger.warning(f"Consensus timeout: {str(e)}")
        raise HTTPException(
            status_code=503,
            detail=ErrorResponse(
                status="error",
                message="Consensus not reached within timeout. Request may have been processed.",
                error_code="CONSENSUS_TIMEOUT",
            ).model_dump(),
        )
    
    except ConsensusTransactionError as e:
        logger.error(f"Consensus error: {str(e)}")
        raise HTTPException(
            status_code=503,
            detail=ErrorResponse(
                status="error",
                message=str(e),
                error_code="CONSENSUS_ERROR",
            ).model_dump(),
        )
    
    except ValueError as e:
        # Domain validation errors (e.g., insufficient funds)
        logger.warning(f"Validation error: {str(e)}")
        raise HTTPException(
            status_code=400,
            detail=ErrorResponse(
                status="error",
                message=str(e),
                error_code="VALIDATION_ERROR",
            ).model_dump(),
        )
    
    except Exception as e:
        logger.error(f"Unexpected error in send_money: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=ErrorResponse(
                status="error",
                message="Internal server error",
                error_code="INTERNAL_ERROR",
            ).model_dump(),
        )


@router.post("/send-consensus", responses={
    400: {"model": ErrorResponse},
    401: {"model": ErrorResponse},
    404: {"model": ErrorResponse},
    409: {"model": ErrorResponse},
    503: {"model": ErrorResponse},
})
async def send_money_consensus(
    request: SendMoneyConsensusRequest,
    db: Session = Depends(get_db_session),
    _: None = Depends(verify_auth),
):
    """Send money via Raft consensus (distributed ledger mode).
    
    This endpoint routes the transaction through the Raft consensus mechanism,
    ensuring it's replicated to multiple nodes before being applied.
    
    Args:
        request: ConsensusTransactionRequest with transfer details and ack level
        db: Database session
        
    Returns:
        JSON response with transaction status
        
    Raises:
        HTTPException: On validation, consensus, or domain errors
    """
    if not raft_module.node:
        raise HTTPException(
            status_code=503,
            detail=ErrorResponse(
                status="error",
                message="Raft consensus not available",
                error_code="CONSENSUS_UNAVAILABLE",
            ).model_dump(),
        )
    
    try:
        # Check idempotency key
        idempotency_mgr = IdempotencyManager(db)
        existing = idempotency_mgr.check_idempotency_key(request.idempotency_key)
        if existing:
            if existing["status"] == "completed":
                logger.info(f"Idempotent retry (consensus): {request.idempotency_key}")
                return {
                    "status": "success",
                    "message": "Money transferred successfully (idempotent)",
                    "transaction_id": existing["transaction_id"]
                }
            elif existing["status"] == "pending":
                raise HTTPException(
                    status_code=409,
                    detail=ErrorResponse(
                        status="error",
                        message="Transaction is still pending",
                        error_code="PENDING",
                    ).model_dump(),
                )
        
        # Create coordinator
        node_id = raft_module.node.node_id
        coordinator = ConsensusedTransactionManager(node_id)
        
        # Prepare transaction data
        transaction_data = {
            "from_account_id": request.from_account_id,
            "to_account_id": request.to_account_id,
            "amount": str(request.amount),
            "currency": request.currency
        }
        
        # Submit to consensus
        success, message, transaction_id = await coordinator.execute_transaction(
            transaction_type="transfer",
            data=transaction_data,
            idempotency_key=request.idempotency_key,
            wait_for_acks=request.wait_for_acks
        )
        
        if not success:
            logger.warning(f"Consensus failed: {message}")
            # If not leader, suggest redirecting
            if "leader" in message.lower():
                raise HTTPException(
                    status_code=307,
                    detail=ErrorResponse(
                        status="error",
                        message=message,
                        error_code="NOT_LEADER",
                    ).model_dump(),
                )
            raise HTTPException(
                status_code=503,
                detail=ErrorResponse(
                    status="error",
                    message=message,
                    error_code="CONSENSUS_FAILED",
                ).model_dump(),
            )
        
        # Register idempotency key as completed
        try:
            idempotency_mgr.register_idempotency_key(request.idempotency_key, transaction_id, node_id)
            idempotency_mgr.mark_completed(request.idempotency_key)
            db.commit()
        except Exception as e:
            logger.warning(f"Failed to register idempotency key: {e}")
        
        logger.info(f"Consensus transaction completed: {transaction_id}")
        return {
            "status": "success",
            "message": message,
            "transaction_id": transaction_id,
            "raft_term": raft_module.node.current_term,
            "leader_id": raft_module.node.get_leader_id()
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in consensus transaction: {e}")
        raise HTTPException(
            status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=ErrorResponse(
                status="error",
                message="Internal server error",
                error_code="INTERNAL_ERROR",
            ).model_dump(),
        )



@router.get("/status/{transaction_id}", tags=["transactions"])
async def get_transaction_status(
    transaction_id: str,
    db: Session = Depends(get_db_session),
    _: None = Depends(verify_auth),
):
    """
    Get the status of a transaction.
    
    Queries the idempotency record to see if a transaction with this ID
    has been applied to the state machine.
    
    Args:
        transaction_id: The transaction ID (UUID)
        db: Database session
        
    Returns:
        Transaction status and metadata
    """
    from src.infrastructure.adapter.persistence.sqlalchemy_models import IdempotencyKeyModel
    
    try:
        record = db.query(IdempotencyKeyModel).filter(
            IdempotencyKeyModel.idempotency_key == transaction_id
        ).first()
        
        if not record:
            return {
                "transaction_id": transaction_id,
                "status": "not_found",
                "message": "Transaction not found",
            }
        
        return {
            "transaction_id": transaction_id,
            "status": record.status,
            "raft_index": record.raft_index,
            "raft_term": record.raft_term,
            "created_at": record.created_at.isoformat() if record.created_at else None,
            "result": record.response_data,
        }
    except Exception as e:
        logger.error(f"Error querying transaction status: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=ErrorResponse(
                status="error",
                message="Internal server error",
                error_code="INTERNAL_ERROR",
            ).model_dump(),
        )


@router.get("/raft-status", tags=["transactions"])
async def get_raft_status(_: None = Depends(verify_auth)):
    """
    Get current Raft consensus status.
    
    Useful for debugging and understanding cluster state.
    
    Returns:
        Node ID, state, term, commit index, log length
    """
    if not raft_module.node:
        raise HTTPException(
            status_code=503,
            detail=ErrorResponse(
                status="error",
                message="Raft not initialized",
                error_code="RAFT_UNAVAILABLE",
            ).model_dump(),
        )
    
    return {
        "node_id": raft_module.node.node_id,
        "state": raft_module.node.state.value,
        "current_term": raft_module.node.current_term,
        "commit_index": raft_module.node.commit_index,
        "last_applied": raft_module.node.last_applied,
        "log_length": len(raft_module.node.log),
        "peers": raft_module.node.peers,
        "is_leader": raft_module.node.state == raft_module.NodeState.LEADER,
    }

