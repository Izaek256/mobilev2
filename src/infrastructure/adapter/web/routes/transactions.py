"""Transaction routes for money transfers."""

from fastapi import APIRouter, Depends, Header, HTTPException, status as http_status
from pydantic import BaseModel
from sqlalchemy.orm import Session
import logging
from typing import Optional

from src.infrastructure.adapter.web.dto import SendMoneyRequest, SendMoneyResponse, ErrorResponse
from src.infrastructure.adapter.web.security import BasicAuthValidator
from src.infrastructure.config.database import get_db_session
from src.infrastructure.adapter.persistence.event_sourced_repositories import (
    EventSourcedAccountRepository,
    EventSourcedTransactionRepository,
    EventSourcedLedgerEntryRepository,
)
from src.infrastructure.config.cluster_state import IdempotencyManager, ClusterStateManager
from src.application.service.send_money_service import SendMoneyService
from src.application.service.transaction_coordinator import ConsensusedTransactionManager
from src.domain.port.inbound.send_money_command import SendMoneyCommand
from src.domain.exception.domain_exception import DomainException
from src.infrastructure.consensus.raft_node import node as raft_node

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1/transactions", tags=["transactions"])

# Simple auth validator (in production, use proper OAuth2)
auth_validator = BasicAuthValidator(
    valid_username="admin",
    valid_password="password123",
)


class ConsensusTransactionRequest(BaseModel):
    """Request for consensus-based transaction."""
    from_account_id: str
    to_account_id: str
    amount: float
    currency: str = "USD"
    idempotency_key: str
    wait_for_acks: int = 1  # Number of nodes to wait for


def verify_auth(authorization: str = Header(None)) -> None:
    """Verify Basic Auth.
    
    Args:
        authorization: Authorization header
        
    Raises:
        HTTPException: If auth is invalid
    """
    auth_validator.validate(authorization)


@router.post("/send", response_model=SendMoneyResponse, responses={
    400: {"model": ErrorResponse},
    401: {"model": ErrorResponse},
    404: {"model": ErrorResponse},
    409: {"model": ErrorResponse},
})
async def send_money(
    request: SendMoneyRequest,
    db: Session = Depends(get_db_session),
    _: None = Depends(verify_auth),
):
    """Send money from one account to another (traditional ACID mode).
    
    Handles:
    - Idempotency using idempotency_key
    - Account validation
    - Sufficient funds check
    - Atomic transaction processing
    - Double-entry bookkeeping
    
    Args:
        request: SendMoneyRequest with transfer details
        db: Database session
        
    Returns:
        SendMoneyResponse with success status
        
    Raises:
        HTTPException: On validation or domain errors
    """
    try:
        # Check idempotency key across cluster
        idempotency_mgr = IdempotencyManager(db)
        existing = idempotency_mgr.check_idempotency_key(request.idempotency_key)
        if existing:
            if existing["status"] == "completed":
                logger.info(f"Idempotent retry: {request.idempotency_key}")
                return SendMoneyResponse(
                    status="success",
                    message="Money transferred successfully (idempotent)",
                )
            elif existing["status"] == "pending":
                raise HTTPException(
                    status_code=409,
                    detail=ErrorResponse(
                        status="error",
                        message="Transaction is still pending",
                        error_code="PENDING",
                    ).model_dump(),
                )
        
        # Create repositories
        account_repo = EventSourcedAccountRepository(db)
        transaction_repo = EventSourcedTransactionRepository(db)
        ledger_repo = EventSourcedLedgerEntryRepository(db)
        
        # Create service
        service = SendMoneyService(account_repo, transaction_repo, ledger_repo)
        
        # Create command
        command = SendMoneyCommand(
            from_account_id=request.from_account_id,
            to_account_id=request.to_account_id,
            amount=request.amount,
            idempotency_key=request.idempotency_key,
        )
        
        # Execute
        service.execute(command)
        
        # Mark as completed
        idempotency_mgr.mark_completed(request.idempotency_key)
        
        # Commit transaction
        db.commit()
        
        logger.info(f"Transaction completed: {request.idempotency_key}")
        return SendMoneyResponse(
            status="success",
            message="Money transferred successfully",
        )
    
    except DomainException as e:
        db.rollback()
        if "idempotency_mgr" in locals():
            try:
                idempotency_mgr.mark_failed(request.idempotency_key, str(e))
                db.commit()
            except:
                pass
        
        # Map domain exceptions to HTTP errors
        error_mapping = {
            "AccountNotFoundException": (404, "ACCOUNT_NOT_FOUND"),
            "InsufficientFundsException": (400, "INSUFFICIENT_FUNDS"),
            "AccountInactiveException": (400, "ACCOUNT_INACTIVE"),
            "InvalidMoneyException": (400, "INVALID_MONEY"),
            "OptimisticLockException": (409, "CONFLICT"),
        }
        
        exception_name = type(e).__name__
        status_code, error_code = error_mapping.get(
            exception_name,
            (500, "INTERNAL_ERROR")
        )
        
        logger.error(f"Domain error in transaction: {exception_name}")
        raise HTTPException(
            status_code=status_code,
            detail=ErrorResponse(
                status="error",
                message=e.message,
                error_code=error_code,
            ).model_dump(),
        )
    
    except Exception as e:
        db.rollback()
        logger.error(f"Error in send_money: {e}")
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
    request: ConsensusTransactionRequest,
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
    if not raft_node:
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
        node_id = raft_node.node_id
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
            "raft_term": raft_node.current_term,
            "leader_id": raft_node.get_leader_id()
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


@router.get("/transaction/{transaction_id}")
async def get_transaction_status(
    transaction_id: str,
    db: Session = Depends(get_db_session),
):
    """Get the status of a transaction.
    
    Args:
        transaction_id: The transaction ID
        db: Database session
        
    Returns:
        Transaction status
    """
    # TODO: Implement transaction status lookup
    return {
        "transaction_id": transaction_id,
        "status": "unknown"
    }

