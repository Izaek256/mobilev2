"""Send Money Use Case - Inbound port interface."""

from abc import ABC, abstractmethod
from src.domain.port.inbound.send_money_command import SendMoneyCommand


class SendMoneyUseCase(ABC):
    """Use case interface for sending money between accounts."""
    
    @abstractmethod
    def execute(self, command: SendMoneyCommand) -> None:
        """Execute the send money use case.
        
        Args:
            command: SendMoneyCommand with transfer details
            
        Raises:
            Various domain exceptions on failure
        """
        pass
