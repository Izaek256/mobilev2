"""Database configuration using SQLAlchemy."""

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker, Session

# Load environment variables from .env file
load_dotenv()

# Base class for ORM models
Base = declarative_base()

# Database URL - defaults to PostgreSQL with psycopg2 driver (sync)
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+psycopg2://postgres:password@localhost:5432/distributed_ledger")

# Create engine - using asyncpg for non-blocking I/O
engine = create_engine(
    DATABASE_URL,
    echo=False,  # Set to True for SQL debugging
)

# Session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db_session() -> Session:
    """Get a database session.
    
    Returns:
        SQLAlchemy Session
    """
    return SessionLocal()


def init_db():
    """Initialize the database, creating all tables."""
    Base.metadata.create_all(bind=engine)
