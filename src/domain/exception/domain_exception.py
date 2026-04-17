"""Domain layer exceptions."""


class DomainException(Exception):
    """Base exception for domain errors."""
    
    def __init__(self, message: str):
        self.message = message
        super().__init__(message)


class AccountNotFoundException(DomainException):
    """Raised when an account is not found."""
    pass


class InsufficientFundsException(DomainException):
    """Raised when an account does not have sufficient balance."""
    pass


class InvalidMoneyException(DomainException):
    """Raised when money object is invalid."""
    pass


class AccountInactiveException(DomainException):
    """Raised when attempting to transact with an inactive account."""
    pass


class TransactionNotFoundException(DomainException):
    """Raised when a transaction is not found."""
    pass


class OptimisticLockException(DomainException):
    """Raised when optimistic lock version conflict occurs."""
    pass
