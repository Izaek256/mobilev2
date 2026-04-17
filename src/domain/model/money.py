"""Money value object with high-precision Decimal."""

from decimal import Decimal
from typing import Union
from src.domain.exception.domain_exception import InvalidMoneyException


class Money:
    """Immutable Money value object using high-precision Decimal."""
    
    def __init__(self, amount: Union[str, int, float, Decimal], currency: str = "USD"):
        """Initialize Money object.
        
        Args:
            amount: Numeric amount as string, int, float, or Decimal
            currency: Currency code (default: USD)
            
        Raises:
            InvalidMoneyException: If amount is negative or invalid
        """
        try:
            self._amount = Decimal(str(amount)).quantize(Decimal("0.01"))
        except Exception as e:
            raise InvalidMoneyException(f"Invalid amount: {e}")
        
        if self._amount < 0:
            raise InvalidMoneyException("Money amount cannot be negative")
        
        self._currency = currency.upper()
    
    @property
    def amount(self) -> Decimal:
        """Get the numeric amount."""
        return self._amount
    
    @property
    def currency(self) -> str:
        """Get the currency code."""
        return self._currency
    
    def add(self, other: "Money") -> "Money":
        """Add money amounts (must be same currency).
        
        Args:
            other: Money to add
            
        Returns:
            New Money object with sum
            
        Raises:
            InvalidMoneyException: If currencies don't match
        """
        if self._currency != other._currency:
            raise InvalidMoneyException(f"Cannot add different currencies: {self._currency} + {other._currency}")
        return Money(self._amount + other._amount, self._currency)
    
    def subtract(self, other: "Money") -> "Money":
        """Subtract money amounts (must be same currency).
        
        Args:
            other: Money to subtract
            
        Returns:
            New Money object with difference
            
        Raises:
            InvalidMoneyException: If currencies don't match or result is negative
        """
        if self._currency != other._currency:
            raise InvalidMoneyException(f"Cannot subtract different currencies: {self._currency} - {other._currency}")
        result = self._amount - other._amount
        if result < 0:
            raise InvalidMoneyException("Result cannot be negative")
        return Money(result, self._currency)
    
    def is_positive(self) -> bool:
        """Check if amount is positive."""
        return self._amount > 0
    
    def is_zero(self) -> bool:
        """Check if amount is zero."""
        return self._amount == 0
    
    def equals(self, other: "Money") -> bool:
        """Check equality."""
        return self._amount == other._amount and self._currency == other._currency
    
    def __str__(self) -> str:
        """String representation."""
        return f"{self._currency} {self._amount}"
    
    def __repr__(self) -> str:
        """Debug representation."""
        return f"Money(amount={self._amount}, currency={self._currency})"
    
    def __eq__(self, other) -> bool:
        """Equality comparison."""
        if not isinstance(other, Money):
            return False
        return self.equals(other)
    
    def __lt__(self, other: "Money") -> bool:
        """Less than comparison (same currency required)."""
        if self._currency != other._currency:
            raise InvalidMoneyException(f"Cannot compare different currencies")
        return self._amount < other._amount
    
    def __le__(self, other: "Money") -> bool:
        """Less than or equal comparison."""
        if self._currency != other._currency:
            raise InvalidMoneyException(f"Cannot compare different currencies")
        return self._amount <= other._amount
    
    def __gt__(self, other: "Money") -> bool:
        """Greater than comparison."""
        if self._currency != other._currency:
            raise InvalidMoneyException(f"Cannot compare different currencies")
        return self._amount > other._amount
    
    def __ge__(self, other: "Money") -> bool:
        """Greater than or equal comparison."""
        if self._currency != other._currency:
            raise InvalidMoneyException(f"Cannot compare different currencies")
        return self._amount >= other._amount
