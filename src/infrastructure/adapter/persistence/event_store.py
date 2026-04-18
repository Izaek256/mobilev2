"""Event Store Implementation."""

import json
from typing import List, Optional
from uuid import UUID
from sqlalchemy.orm import Session
from src.infrastructure.adapter.persistence.sqlalchemy_models import EventLogModel
from src.domain.model.event import (
    DomainEvent,
    AccountCreatedEvent,
    MoneyDepositedEvent,
    MoneyWithdrawnEvent,
    TransactionCreatedEvent,
    TransactionCompletedEvent,
    TransactionFailedEvent,
)

# Registry for deserialization
EVENT_REGISTRY = {
    "AccountCreatedEvent": AccountCreatedEvent,
    "MoneyDepositedEvent": MoneyDepositedEvent,
    "MoneyWithdrawnEvent": MoneyWithdrawnEvent,
    "TransactionCreatedEvent": TransactionCreatedEvent,
    "TransactionCompletedEvent": TransactionCompletedEvent,
    "TransactionFailedEvent": TransactionFailedEvent,
}

class EventStore:
    """Event Store for Event Sourcing."""
    
    def __init__(self, session: Session):
        self.session = session
        
    def save_event(self, event: DomainEvent, aggregate_id: str) -> None:
        """Save a single event to the event store."""
        event_dict = event.__dict__.copy()
        # Convert datetime to string for json serialization
        if "timestamp" in event_dict:
            event_dict["timestamp"] = event_dict["timestamp"].isoformat()
            
        payload = json.dumps(event_dict)
        
        model = EventLogModel(
            event_id=event.event_id,
            aggregate_id=aggregate_id,
            event_type=event.event_type,
            payload=payload,
            timestamp=event.timestamp
        )
        
        self.session.add(model)
        self.session.flush()
        
    def get_events_for_aggregate(self, aggregate_id: str) -> List[DomainEvent]:
        """Retrieve all events for an aggregate, ordered by index."""
        models = self.session.query(EventLogModel).filter(
            EventLogModel.aggregate_id == aggregate_id
        ).order_by(EventLogModel.log_index).all()
        
        events = []
        for model in models:
            event_cls = EVENT_REGISTRY.get(model.event_type)
            if not event_cls:
                continue
                
            payload_dict = json.loads(model.payload)
            # Reconstruct event
            from datetime import datetime
            if "timestamp" in payload_dict:
                payload_dict["timestamp"] = datetime.fromisoformat(payload_dict["timestamp"])
                
            event = event_cls(**payload_dict)
            events.append(event)
            
        return events
