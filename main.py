"""Application entry point."""

import os
import uvicorn
from src.infrastructure.adapter.web.http_adapter import create_app
from src.infrastructure.config.database import init_db


def main():
    """Main entry point."""
    # Initialize database (create tables if not exist)
    # init_db()  # Temporarily commented out - requires active PostgreSQL connection
    
    # Create FastAPI app
    app = create_app()
    
    # Get port from environment variable, default to 8000
    port = int(os.getenv("PORT", "8000"))
    
    # Run with uvicorn
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=port,
        log_level="info",
    )


if __name__ == "__main__":
    main()
