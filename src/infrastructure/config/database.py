"""Database configuration using SQLAlchemy."""

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker, Session

# Load environment variables from .env file
load_dotenv()

# Base class for ORM models
Base = declarative_base()

# Global engine and session factory - will be initialized on first use
engine = None
SessionLocal = None


def _get_database_url() -> str:
    """Get the database URL from environment or use default."""
    return os.getenv("DATABASE_URL", "mysql+aiomysql://root@localhost:3306/distributed_ledger")


def _init_engine():
    """Initialize the SQLAlchemy engine and session factory."""
    global engine, SessionLocal
    if engine is None:
        database_url = _get_database_url()
        print(f"Initializing database with URL: {database_url}")
        # Create engine - using aiomysql for non-blocking I/O
        engine = create_engine(
            database_url,
            echo=False,  # Set to True for SQL debugging
            pool_pre_ping=True,  # Verify connections before using them
        )
        # Session factory
        SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db_session() -> Session:
    """Get a database session.
    
    Returns:
        SQLAlchemy Session
    """
    _init_engine()
    return SessionLocal()


def init_db():
    """Initialize the database, creating all tables."""
    _init_engine()
    Base.metadata.create_all(bind=engine)
