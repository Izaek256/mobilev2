"""Data Transfer Objects (DTOs) for HTTP API."""

from uuid import UUID
from decimal import Decimal
from pydantic import BaseModel, Field


class SendMoneyRequest(BaseModel):
    """Request DTO for sending money between accounts."""
    
    from_account_id: str = Field(..., description="Source account ID")
    to_account_id: str = Field(..., description="Destination account ID")
    amount: Decimal = Field(..., gt=0, description="Amount to transfer (must be positive)")
    idempotency_key: str = Field(..., min_length=1, description="Unique idempotency key")
    
    class Config:
        json_schema_extra = {
            "example": {
                "from_account_id": "alice",
                "to_account_id": "bob",
                "amount": 100.50,
                "idempotency_key": "txn-12345-67890",
            }
        }


class SendMoneyResponse(BaseModel):
    """Response DTO for send money operation."""
    
    status: str = Field(..., description="Operation status (e.g., 'success')")
    message: str = Field(..., description="Response message")
    
    class Config:
        json_schema_extra = {
            "example": {
                "status": "success",
                "message": "Money transferred successfully",
            }
        }


class HealthResponse(BaseModel):
    """Response DTO for health check endpoint."""
    
    status: str = Field(..., description="Health status (e.g., 'healthy')")
    message: str = Field(..., description="Health check message")
    
    class Config:
        json_schema_extra = {
            "example": {
                "status": "healthy",
                "message": "Distributed Ledger System is running",
            }
        }


class ErrorResponse(BaseModel):
    """Response DTO for error responses."""
    
    status: str = Field(..., description="Error status (e.g., 'error')")
    message: str = Field(..., description="Error message")
    error_code: str = Field(..., description="Error code")
    
    class Config:
        json_schema_extra = {
            "example": {
                "status": "error",
                "message": "Insufficient funds in source account",
                "error_code": "INSUFFICIENT_FUNDS",
            }
        }
