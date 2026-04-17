# Distributed Ledger System

A robust, simple distributed ledger system built with Python, FastAPI, and PostgreSQL. Handles secure money transfers with ACID guarantees, double-entry bookkeeping, and high-throughput concurrent transaction processing.

## Architecture

This project follows **Hexagonal Architecture** (Ports & Adapters) with three layers:

- **Domain Layer**: Pure Python business logic (models, entities, exceptions)
- **Application Layer**: Use cases and service orchestration
- **Infrastructure Layer**: Framework implementations (FastAPI, SQLAlchemy, PostgreSQL)

## Prerequisites

- **Python 3.12+** (tested with Python 3.12+)
- **PostgreSQL 12+** installed and running locally
- `pip` package manager

## Setup

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/distributed-ledger-python.git
cd distributed-ledger-python
```

### 2. Create Virtual Environment

```bash
python -m venv venv

# Activate virtual environment
# On Windows:
venv\Scripts\activate
# On Linux/Mac:
source venv/bin/activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure Database

**Option A: Using PostgreSQL (Recommended)**

1. Create a PostgreSQL database:
   ```sql
   CREATE DATABASE distributed_ledger;
   ```

2. Create `.env` file from the example:
   ```bash
   cp .env.example .env
   ```

3. Update `.env` with your PostgreSQL credentials:
   ```env
   DATABASE_URL=postgresql+asyncpg://postgres:your_password@localhost:5432/distributed_ledger
   ```

**Option B: Using SQLite (Local Testing)**

Update `DATABASE_URL` in `.env`:
```env
DATABASE_URL=sqlite:///./ledger.db
```

### 5. Run the Application

```bash
python main.py
```

The server will start at `http://localhost:8000`

## API Endpoints

### Health Check
```bash
GET http://localhost:8000/health
```

### Send Money (Authenticated)
```bash
POST http://localhost:8000/api/v1/transactions/send
Authorization: Basic base64(username:password)
Content-Type: application/json

{
  "from_account_id": "account-uuid",
  "to_account_id": "account-uuid",
  "amount": 100.50,
  "currency": "USD",
  "idempotency_key": "unique-transaction-id"
}
```

## Project Structure

```
distributed-ledger-python/
├── src/
│   ├── domain/                      # Business logic (framework-independent)
│   │   ├── model/                   # Domain entities
│   │   ├── port/                    # Input/Output ports
│   │   └── exception/               # Custom exceptions
│   ├── application/                 # Use cases & services
│   │   └── service/
│   └── infrastructure/              # Framework implementations
│       ├── adapter/
│       │   ├── persistence/         # Database adapters
│       │   └── web/                 # HTTP adapters & routes
│       └── config/                  # Database & DI config
├── main.py                          # Application entry point
├── requirements.txt                 # Python dependencies
├── .env.example                     # Environment template
└── README.md                         # This file
```

## Key Features

- ✅ **ACID Transactions**: Guaranteed consistency with database transactions
- ✅ **Double-Entry Bookkeeping**: Every transfer creates DEBIT/CREDIT ledger entries
- ✅ **Idempotency**: Repeated requests with same reference don't duplicate charges
- ✅ **Optimistic Locking**: Version-based conflict detection for concurrent updates
- ✅ **HTTP Basic Auth**: Simple authentication for API access
- ✅ **Hexagonal Architecture**: Clean separation of concerns, highly testable

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DATABASE_URL` | PostgreSQL connection string | `postgresql+asyncpg://postgres:password@localhost:5432/distributed_ledger` |

## Development

### Enable SQL Logging

In [src/infrastructure/config/database.py](src/infrastructure/config/database.py), set `echo=True`:

```python
engine = create_engine(
    DATABASE_URL,
    echo=True,  # Enable SQL query logging
)
```

### Database Schema

The application automatically creates all tables on startup via SQLAlchemy's `create_all()`.

Tables created:
- `accounts` - Account information and balances
- `transactions` - Transfer transaction records
- `ledger_entries` - Double-entry bookkeeping entries (DEBIT/CREDIT)

## Testing

Manual testing with curl:

```bash
# Create test account (if endpoint available)
curl -X POST http://localhost:8000/api/v1/accounts \
  -H "Content-Type: application/json" \
  -d '{"account_id": "acc-1", "owner": "Alice", "balance": 1000}'

# Send money
curl -X POST http://localhost:8000/api/v1/transactions/send \
  -H "Authorization: Basic dXNlcm5hbWU6cGFzc3dvcmQ=" \
  -H "Content-Type: application/json" \
  -d '{
    "from_account_id": "acc-1",
    "to_account_id": "acc-2",
    "amount": 100.00,
    "currency": "USD",
    "idempotency_key": "txn-001"
  }'
```

## Troubleshooting

### PostgreSQL Connection Error
- Ensure PostgreSQL is running: `psql -U postgres`
- Check connection string in `.env`
- Verify database exists: `psql -l | grep distributed_ledger`

### Module Import Errors
- Ensure virtual environment is activated
- Run: `pip install -r requirements.txt`
- Check Python version: `python --version` (should be 3.12+)

### Port Already in Use
- Change port in `main.py`: `uvicorn.run(app, port=8001)`

## Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/your-feature`
3. Commit changes: `git commit -am 'Add feature'`
4. Push to branch: `git push origin feature/your-feature`
5. Open a Pull Request

## License

MIT License - see LICENSE file for details

## Author

Created as a simplified distributed ledger implementation following Hexagonal Architecture principles.
