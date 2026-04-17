"""Web security utilities."""

import base64
from typing import Optional, Tuple
from fastapi import HTTPException, status


class BasicAuthValidator:
    """Validates HTTP Basic Authentication."""
    
    def __init__(self, valid_username: str, valid_password: str):
        """Initialize validator with credentials.
        
        Args:
            valid_username: Valid username
            valid_password: Valid password
        """
        self.valid_username = valid_username
        self.valid_password = valid_password
    
    def validate(self, auth_header: Optional[str]) -> bool:
        """Validate Basic Auth header.
        
        Args:
            auth_header: Authorization header value
            
        Returns:
            True if valid, raises HTTPException if invalid
            
        Raises:
            HTTPException: If auth is missing or invalid
        """
        if not auth_header:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Missing authorization header",
                headers={"WWW-Authenticate": "Basic"},
            )
        
        try:
            scheme, credentials = auth_header.split(" ")
            if scheme.lower() != "basic":
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Invalid authentication scheme",
                    headers={"WWW-Authenticate": "Basic"},
                )
            
            decoded = base64.b64decode(credentials).decode("utf-8")
            username, password = decoded.split(":")
            
            if username == self.valid_username and password == self.valid_password:
                return True
            
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid credentials",
                headers={"WWW-Authenticate": "Basic"},
            )
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid authorization header format",
                headers={"WWW-Authenticate": "Basic"},
            )
