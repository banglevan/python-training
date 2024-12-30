"""
Security and authentication module.
"""

import logging
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
from passlib.context import CryptContext
from jose import JWTError, jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer

from .config import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Password hashing
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# OAuth2 scheme
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

class SecurityManager:
    """Security management."""
    
    def __init__(self):
        """Initialize security manager."""
        self.secret_key = config.get('security.secret_key')
        self.algorithm = config.get('security.algorithm', 'HS256')
        self.access_token_expire_minutes = config.get(
            'security.access_token_expire_minutes',
            30
        )
    
    def verify_password(
        self,
        plain_password: str,
        hashed_password: str
    ) -> bool:
        """Verify password hash."""
        return pwd_context.verify(plain_password, hashed_password)
    
    def get_password_hash(self, password: str) -> str:
        """Generate password hash."""
        return pwd_context.hash(password)
    
    def create_access_token(
        self,
        data: Dict[str, Any],
        expires_delta: Optional[timedelta] = None
    ) -> str:
        """Create JWT access token."""
        try:
            to_encode = data.copy()
            
            if expires_delta:
                expire = datetime.utcnow() + expires_delta
            else:
                expire = datetime.utcnow() + timedelta(
                    minutes=self.access_token_expire_minutes
                )
            
            to_encode.update({"exp": expire})
            encoded_jwt = jwt.encode(
                to_encode,
                self.secret_key,
                algorithm=self.algorithm
            )
            
            return encoded_jwt
            
        except Exception as e:
            logger.error(f"Token creation failed: {e}")
            raise
    
    async def get_current_user(
        self,
        token: str = Depends(oauth2_scheme)
    ) -> Dict[str, Any]:
        """Get current user from token."""
        credentials_exception = HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
        
        try:
            payload = jwt.decode(
                token,
                self.secret_key,
                algorithms=[self.algorithm]
            )
            username: str = payload.get("sub")
            if username is None:
                raise credentials_exception
            
            return {"username": username, "role": payload.get("role")}
            
        except JWTError:
            raise credentials_exception
    
    def check_permissions(
        self,
        required_role: str,
        user_role: str
    ) -> bool:
        """Check user permissions."""
        role_hierarchy = {
            'admin': 3,
            'editor': 2,
            'viewer': 1
        }
        
        return role_hierarchy.get(user_role, 0) >= role_hierarchy.get(required_role, 0)

# Global security manager instance
security_manager = SecurityManager() 