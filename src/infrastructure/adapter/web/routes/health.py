"""Health check routes."""

from fastapi import APIRouter

from src.infrastructure.adapter.web.dto import HealthResponse

router = APIRouter(tags=["health"])


@router.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint.
    
    Returns:
        HealthResponse with status
    """
    return HealthResponse(
        status="healthy",
        message="Distributed Ledger System is running",
    )
