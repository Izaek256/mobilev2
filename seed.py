"""
Seed script — creates test accounts for the Mobile Money Wallet.
Run once: python seed.py
"""

import uuid
from decimal import Decimal
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

from src.infrastructure.config.database import SessionLocal, init_db
from src.infrastructure.adapter.persistence.sqlalchemy_models import AccountModel

# ── Seed data ──────────────────────────────────────────────────────────────────

ACCOUNTS = [
    {
        "account_id": uuid.UUID("aaaaaaaa-0000-0000-0000-000000000001"),
        "owner_id":   "alice",
        "balance":    Decimal("500000.00"),   # UGX 500,000
        "currency":   "UGX",
        "status":     "ACTIVE",
    },
    {
        "account_id": uuid.UUID("bbbbbbbb-0000-0000-0000-000000000002"),
        "owner_id":   "bob",
        "balance":    Decimal("250000.00"),   # UGX 250,000
        "currency":   "UGX",
        "status":     "ACTIVE",
    },
    {
        "account_id": uuid.UUID("cccccccc-0000-0000-0000-000000000003"),
        "owner_id":   "carol",
        "balance":    Decimal("100000.00"),   # UGX 100,000
        "currency":   "UGX",
        "status":     "ACTIVE",
    },
    {
        "account_id": uuid.UUID("dddddddd-0000-0000-0000-000000000004"),
        "owner_id":   "dave",
        "balance":    Decimal("0.00"),
        "currency":   "UGX",
        "status":     "ACTIVE",
    },
]


def run():
    print("Initializing database tables...")
    init_db()
    print("Tables OK.\n")

    db = SessionLocal()
    try:
        created = 0
        skipped = 0
        for data in ACCOUNTS:
            existing = db.query(AccountModel).filter_by(account_id=data["account_id"]).first()
            if existing:
                print(f"  [skip]  {data['owner_id']:8s}  {data['account_id']}  (already exists)")
                skipped += 1
                continue

            acct = AccountModel(
                account_id=data["account_id"],
                owner_id=data["owner_id"],
                balance=data["balance"],
                currency=data["currency"],
                status=data["status"],
                version=0,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
            )
            db.add(acct)
            created += 1
            print(f"  [add]   {data['owner_id']:8s}  {data['account_id']}")

        db.commit()

        print(f"\nDone — {created} created, {skipped} skipped.\n")
        print("=" * 65)
        print("  ACCOUNT IDs TO USE IN THE CLIENT")
        print("=" * 65)
        for data in ACCOUNTS:
            bal = f"UGX {data['balance']:>12,.0f}"
            print(f"  {data['owner_id']:8s}  {data['account_id']}  {bal}")
        print("=" * 65)

    except Exception as e:
        db.rollback()
        print(f"\nERROR: {e}")
    finally:
        db.close()


if __name__ == "__main__":
    run()
