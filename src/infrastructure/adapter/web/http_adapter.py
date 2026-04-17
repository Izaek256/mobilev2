"""FastAPI HTTP adapter setup."""

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError

from src.infrastructure.adapter.web.routes import health, transactions
from src.infrastructure.adapter.web.dto import ErrorResponse


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
