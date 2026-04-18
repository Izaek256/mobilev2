"""Account routes."""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from src.infrastructure.config.database import get_db_session
from src.infrastructure.adapter.persistence.event_sourced_repositories import EventSourcedAccountRepository
from src.domain.model.account import Account

router = APIRouter(prefix="/api/v1/accounts", tags=["accounts"])

@router.get("/{account_id}")
async def get_account(account_id: str, db: Session = Depends(get_db_session)):
    repo = EventSourcedAccountRepository(db)
    account = repo.find_by_id(account_id)
    if not account:
        raise HTTPException(status_code=404, detail="Account not found")
        
    return {
        "account_id": account.account_id,
        "owner_id": account.owner_id,
        "balance": float(account.balance.amount),
        "currency": account.balance.currency,
        "status": account.status.value if hasattr(account.status, 'value') else str(account.status),
        "created_at": account.created_at.isoformat() if account.created_at else None,
        "updated_at": account.updated_at.isoformat() if account.updated_at else None,
    }

@router.get("/{account_id}/balance")
async def get_account_balance(account_id: str, db: Session = Depends(get_db_session)):
    repo = EventSourcedAccountRepository(db)
    account = repo.find_by_id(account_id)
    if not account:
        raise HTTPException(status_code=404, detail="Account not found")
        
    return {
        "account_id": account.account_id,
        "balance": float(account.balance.amount),
        "currency": account.balance.currency,
    }
