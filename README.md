# Distributed Ledger System

A robust distributed ledger system built with Python, FastAPI, and PostgreSQL. Implements Raft consensus for multi-node replication, event sourcing for transaction history, and distributed consistency guarantees.

## Architecture

This project follows **Hexagonal Architecture** (Ports & Adapters) with three layers:

- **Domain Layer**: Pure Python business logic (models, entities, exceptions)
- **Application Layer**: Use cases and service orchestration (including Raft consensus coordination)
- **Infrastructure Layer**: Framework implementations (FastAPI, SQLAlchemy, PostgreSQL, Raft consensus engine)

## Key Distributed Ledger Features

### Phase 1: Event Sourcing ✅
- Immutable event log of all state changes
- Event replay for audit trails and recovery
- Event-sourced repositories for accounts and transactions
- Full transaction history available through event store

### Phase 2: Replication & Synchronization ✅
- Write-Ahead Log (WAL) for durability
- Persistent replication log in database
- Node-to-node log replication protocol
- Automatic catch-up mechanism for rejoining nodes

### Phase 3: Raft Consensus ✅
- Full Raft consensus implementation
- Leader election with randomized timeouts
- Log consistency verification with log matching
- Follower state tracking (nextIndex, matchIndex)
- Automatic term advancement and leader discovery
- Proper handling of network partitions

### Phase 4: Multi-Node Orchestration ✅
- Cluster state management and peer tracking
- Node registration and discovery
- Health checks and node status monitoring
- Cluster quorum calculation

### Phase 5: Transaction Coordination ✅
- Idempotent write deduplication across cluster
- Consensus-based transaction routing through leader
- Replication acknowledgment tracking
- Configurable write acknowledgment levels

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

2. Create `.env` file with the following content:
   ```env
   DATABASE_URL=postgresql+asyncpg://postgres:your_password@localhost:5432/distributed_ledger
   NODE_ID=node-1
   PEERS=http://node-2:8000,http://node-3:8000
   ```

3. For multi-node setup, update `NODE_ID` and `PEERS` accordingly.

**Option B: Using SQLite (Local Testing - Single Node Only)**

```env
DATABASE_URL=sqlite:///./ledger.db
NODE_ID=node-1
PEERS=
```

### 5. Run the Application

```bash
# Single node (auto-becomes leader)
python main.py

# Multi-node (ensure all nodes can reach each other)
# Node 1
NODE_ID=node-1 PEERS="http://localhost:8001,http://localhost:8002" python main.py

# Node 2 (in another terminal)
NODE_ID=node-2 PEERS="http://localhost:8000,http://localhost:8002" python main.py

# Node 3 (in another terminal)
NODE_ID=node-3 PEERS="http://localhost:8000,http://localhost:8001" python main.py
```

The servers will start at `http://localhost:8000`, `http://localhost:8001`, `http://localhost:8002` respectively.

## API Endpoints

### Health Check
```bash
GET http://localhost:8000/health
```

### Raft Cluster Status
```bash
# Get node status
GET http://localhost:8000/raft/status

# Get entire cluster status
GET http://localhost:8000/raft/cluster/status

# Get cluster leader
GET http://localhost:8000/raft/cluster/leader

# Get cluster health
GET http://localhost:8000/raft/cluster/health
```

### Traditional Transaction (ACID Mode)
```bash
POST http://localhost:8000/api/v1/transactions/send
Authorization: Basic dXNlcm5hbWU6cGFzc3dvcmQ=
Content-Type: application/json

{
  "from_account_id": "account-uuid",
  "to_account_id": "account-uuid",
  "amount": 100.50,
  "currency": "USD",
  "idempotency_key": "unique-transaction-id"
}
```

### Consensus-Based Transaction (Distributed Ledger Mode)
```bash
POST http://localhost:8000/api/v1/transactions/send-consensus
Authorization: Basic dXNlcm5hbWU6cGFzc3dvcmQ=
Content-Type: application/json

{
  "from_account_id": "account-uuid",
  "to_account_id": "account-uuid",
  "amount": 100.50,
  "currency": "USD",
  "idempotency_key": "unique-transaction-id",
  "wait_for_acks": 2
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
│   │       ├── send_money_service.py
│   │       ├── transaction_coordinator.py  # Raft integration
│   │       └── transfer_executor.py
│   └── infrastructure/              # Framework implementations
│       ├── adapter/
│       │   ├── persistence/
│       │   │   ├── event_store.py
│       │   │   ├── wal.py           # Write-Ahead Log
│       │   │   └── repositories.py
│       │   └── web/
│       │       ├── routes/
│       │       │   ├── cluster.py    # Cluster API
│       │       │   └── transactions.py
│       │       └── http_adapter.py
│       ├── config/
│       │   ├── database.py
│       │   └── cluster_state.py     # Cluster state management
│       └── consensus/
│           └── raft_node.py         # Raft implementation
├── main.py                          # Application entry point
├── requirements.txt                 # Python dependencies
├── .env.example                     # Environment template
└── README.md                         # This file
```

## Key Features

### Event Sourcing
- ✅ Immutable append-only event log
- ✅ Complete transaction history
- ✅ Event replay capabilities
- ✅ Point-in-time state reconstruction

### Raft Consensus
- ✅ Full leader election protocol
- ✅ Log replication with quorum confirmation
- ✅ Network partition tolerance
- ✅ Automatic failover
- ✅ Proper log consistency checking

### Distributed Consistency
- ✅ Idempotent transaction deduplication across cluster
- ✅ Eventual consistency guarantees
- ✅ Quorum-based writes with acknowledgment levels
- ✅ Double-entry bookkeeping

### Reliability
- ✅ Write-Ahead Logging (WAL) for crash recovery
- ✅ Transaction persistence across node failures
- ✅ Automatic cluster healing
- ✅ Health monitoring and node status tracking

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DATABASE_URL` | PostgreSQL connection string | `postgresql+asyncpg://postgres:password@localhost:5432/distributed_ledger` |
| `NODE_ID` | This node's unique identifier | `node-1` |
| `PEERS` | Comma-separated list of peer addresses | Empty (single-node) |

## Running Multi-Node Cluster

### Setup 3-Node Cluster (Recommended for HA)

1. **Terminal 1 - Node 1:**
```bash
NODE_ID=node-1 \
PEERS="http://localhost:8001,http://localhost:8002" \
python main.py
```

2. **Terminal 2 - Node 2:**
```bash
NODE_ID=node-2 \
PEERS="http://localhost:8000,http://localhost:8002" \
DATABASE_URL=postgresql+asyncpg://postgres:password@localhost:5432/distributed_ledger_2 \
python -c "from src.infrastructure.config.database import init_db; init_db()" && \
uvicorn src.infrastructure.adapter.web.http_adapter:create_app --host 0.0.0.0 --port 8001
```

3. **Terminal 3 - Node 3:**
```bash
NODE_ID=node-3 \
PEERS="http://localhost:8000,http://localhost:8001" \
DATABASE_URL=postgresql+asyncpg://postgres:password@localhost:5432/distributed_ledger_3 \
python -c "from src.infrastructure.config.database import init_db; init_db()" && \
uvicorn src.infrastructure.adapter.web.http_adapter:create_app --host 0.0.0.0 --port 8002
```

### Verify Cluster

```bash
# Check leader
curl http://localhost:8000/raft/cluster/leader

# Check cluster health
curl http://localhost:8000/raft/cluster/health

# List all nodes
curl http://localhost:8000/raft/cluster/nodes
```

## Failure Scenarios & Recovery

### Node Failure
- Other nodes detect failure via heartbeat timeout
- If leader fails, new leader is elected automatically
- Failed node can rejoin and catch up from log

### Network Partition
- Nodes in minority partition step down (can't reach quorum)
- Nodes in majority partition continue to serve
- Upon partition healing, nodes resync via log replication

### Concurrent Writes to Different Nodes
- All writes are routed through leader via Raft consensus
- Followers reject write attempts and indicate leader address
- Idempotency keys prevent duplicate processing

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
- `event_log` - Event sourcing log
- `raft_log` - Raft consensus log (Write-Ahead Log)
- `cluster_state` - Cluster peer metadata
- `idempotency_keys` - Transaction deduplication across cluster

## Testing

### Manual Testing with cURL

```bash
# Check cluster status
curl http://localhost:8000/raft/cluster/status | jq

# Send consensus transaction (auto-routes to leader)
curl -X POST http://localhost:8000/api/v1/transactions/send-consensus \
  -H "Authorization: Basic YWRtaW46cGFzc3dvcmQxMjM=" \
  -H "Content-Type: application/json" \
  -d '{
    "from_account_id": "account-1",
    "to_account_id": "account-2",
    "amount": 100.00,
    "currency": "USD",
    "idempotency_key": "txn-001",
    "wait_for_acks": 2
  }' | jq

# Repeat transaction (should return idempotent result)
curl -X POST http://localhost:8000/api/v1/transactions/send-consensus \
  -H "Authorization: Basic YWRtaW46cGFzc3dvcmQxMjM=" \
  -H "Content-Type: application/json" \
  -d '{
    "from_account_id": "account-1",
    "to_account_id": "account-2",
    "amount": 100.00,
    "currency": "USD",
    "idempotency_key": "txn-001",
    "wait_for_acks": 2
  }' | jq
```

### Chaos Testing

Kill a node and verify:

```bash
# Terminal: Kill Node 2 (Ctrl+C)
# Check cluster status - should show Node 2 unhealthy
curl http://localhost:8000/raft/cluster/health | jq

# Send a transaction - should still work with 2 healthy nodes
curl -X POST http://localhost:8000/api/v1/transactions/send-consensus ...

# Restart Node 2
# Node 2 will replay missing log entries and rejoin cluster
```

## Troubleshooting

### No Leader Elected
- Ensure all nodes have reachable peer addresses
- Check firewall allows communication between nodes
- Verify clock synchronization between nodes
- Check logs for election timeout issues

### Replication Lag
- Check network connectivity between nodes
- Monitor `raft_log` table for log entry accumulation
- Verify database write performance on all nodes

### Idempotency Errors
- Ensure `idempotency_key` is unique across retries
- Check `idempotency_keys` table for duplicate keys
- Verify database replication across nodes

### PostgreSQL Connection Error
- Ensure PostgreSQL is running: `psql -U postgres`
- Check connection string in `.env`
- Verify database exists: `psql -l | grep distributed_ledger`

### Module Import Errors
- Ensure virtual environment is activated
- Run: `pip install -r requirements.txt`
- Check Python version: `python --version` (should be 3.12+)

## Production Deployment Considerations

1. **Security**: Replace Basic Auth with OAuth2/JWT
2. **Monitoring**: Add Prometheus metrics and alerting
3. **Logging**: Implement structured logging with aggregation
4. **Backup**: Regular snapshot backups of event log
5. **Scaling**: Add read replicas for query load balancing
6. **Recovery**: Implement automatic log compaction/snapshots

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
