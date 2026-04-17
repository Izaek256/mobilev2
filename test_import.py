#!/usr/bin/env python
"""Test script to import all modules and identify errors."""

import sys
import traceback

try:
    print("Importing infrastructure modules...")
    from src.infrastructure.adapter.web.http_adapter import create_app
    from src.infrastructure.config.container import Container
    from src.infrastructure.adapter.persistence.repositories import (
        SQLAlchemyAccountRepository,
        SQLAlchemyTransactionRepository,
        SQLAlchemyLedgerEntryRepository,
    )
    
    print("Creating app...")
    app = create_app()
    
    print("Getting service...")
    service = Container.get_send_money_service()
    
    print("SUCCESS - All modules imported and initialized successfully")
    
except Exception as e:
    print(f"ERROR: {type(e).__name__}: {e}")
    traceback.print_exc()
    sys.exit(1)
