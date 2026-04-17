"""Transaction routes for money transfers."""

from fastapi import APIRouter, Depends, Header, HTTPException, status
from sqlalchemy.orm import Session

from src.infrastructure.adapter.web.dto import SendMoneyRequest, SendMoneyResponse, ErrorResponse
from src.infrastructure.adapter.web.security import BasicAuthValidator
from src.infrastructure.config.database import get_db_session
from src.infrastructure.adapter.persistence.repositories import (
    SQLAlchemyAccountRepository,
    SQLAlchemyTransactionRepository,
    SQLAlchemyLedgerEntryRepository,
)
from src.application.service.send_money_service import SendMoneyService
from src.domain.port.inbound.send_money_command import SendMoneyCommand
from src.domain.exception.domain_exception import DomainException

router = APIRouter(prefix="/api/v1/transactions", tags=["transactions"])

# Simple auth validator (in production, use proper OAuth2)
auth_validator = BasicAuthValidator(
    valid_username="admin",
    valid_password="password123",
)


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
    """Send money from one account to another.
    
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
        # Create repositories
        account_repo = SQLAlchemyAccountRepository(db)
        transaction_repo = SQLAlchemyTransactionRepository(db)
        ledger_repo = SQLAlchemyLedgerEntryRepository(db)
        
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
        
        # Commit transaction
        db.commit()
        
        return SendMoneyResponse(
            status="success",
            message="Money transferred successfully",
        )
    
    except DomainException as e:
        db.rollback()
        
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
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=ErrorResponse(
                status="error",
                message="Internal server error",
                error_code="INTERNAL_ERROR",
            ).model_dump(),
        )
