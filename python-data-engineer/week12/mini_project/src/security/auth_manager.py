"""
Authentication and authorization management.
"""

from typing import Dict, Any, Optional, List
import logging
from datetime import datetime, timedelta
import jwt
from passlib.hash import pbkdf2_sha256
from fastapi import HTTPException, Security
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AuthManager:
    """Authentication management."""
    
    def __init__(
        self,
        secret_key: str,
        token_expiry: timedelta = timedelta(hours=24)
    ):
        """Initialize manager."""
        self.secret_key = secret_key
        self.token_expiry = token_expiry
        self.users = {}
        self.roles = {}
        self.security = HTTPBearer()
    
    def create_user(
        self,
        username: str,
        password: str,
        roles: List[str] = ["user"]
    ) -> None:
        """
        Create user.
        
        Args:
            username: Username
            password: Password
            roles: User roles
        """
        try:
            if username in self.users:
                raise ValueError(f"User already exists: {username}")
            
            # Hash password
            hashed_password = pbkdf2_sha256.hash(password)
            
            # Store user
            self.users[username] = {
                'password': hashed_password,
                'roles': roles,
                'created_at': datetime.now().isoformat(),
                'last_login': None
            }
            
            logger.info(f"Created user: {username}")
            
        except Exception as e:
            logger.error(f"Failed to create user: {e}")
            raise
    
    def create_role(
        self,
        name: str,
        permissions: List[str]
    ) -> None:
        """
        Create role.
        
        Args:
            name: Role name
            permissions: Role permissions
        """
        try:
            if name in self.roles:
                raise ValueError(f"Role already exists: {name}")
            
            self.roles[name] = {
                'permissions': permissions,
                'created_at': datetime.now().isoformat()
            }
            
            logger.info(f"Created role: {name}")
            
        except Exception as e:
            logger.error(f"Failed to create role: {e}")
            raise
    
    def authenticate(
        self,
        username: str,
        password: str
    ) -> str:
        """
        Authenticate user.
        
        Args:
            username: Username
            password: Password
        
        Returns:
            JWT token
        """
        try:
            if username not in self.users:
                raise ValueError("Invalid credentials")
            
            user = self.users[username]
            
            # Verify password
            if not pbkdf2_sha256.verify(
                password,
                user['password']
            ):
                raise ValueError("Invalid credentials")
            
            # Generate token
            payload = {
                'sub': username,
                'roles': user['roles'],
                'exp': datetime.utcnow() + self.token_expiry
            }
            
            token = jwt.encode(
                payload,
                self.secret_key,
                algorithm='HS256'
            )
            
            # Update last login
            user['last_login'] = datetime.now().isoformat()
            
            return token
            
        except Exception as e:
            logger.error(f"Authentication failed: {e}")
            raise
    
    def verify_token(
        self,
        credentials: HTTPAuthorizationCredentials = Security(HTTPBearer())
    ) -> Dict[str, Any]:
        """
        Verify JWT token.
        
        Args:
            credentials: HTTP credentials
        
        Returns:
            Token payload
        """
        try:
            token = credentials.credentials
            payload = jwt.decode(
                token,
                self.secret_key,
                algorithms=['HS256']
            )
            
            if payload['sub'] not in self.users:
                raise ValueError("Invalid token")
            
            return payload
            
        except jwt.ExpiredSignatureError:
            raise HTTPException(
                status_code=401,
                detail="Token has expired"
            )
        except jwt.InvalidTokenError:
            raise HTTPException(
                status_code=401,
                detail="Invalid token"
            )
    
    def check_permission(
        self,
        token_payload: Dict[str, Any],
        required_permission: str
    ) -> bool:
        """
        Check permission.
        
        Args:
            token_payload: Token payload
            required_permission: Required permission
        
        Returns:
            Whether user has permission
        """
        try:
            user_roles = token_payload['roles']
            
            for role in user_roles:
                if role not in self.roles:
                    continue
                    
                if required_permission in self.roles[role]['permissions']:
                    return True
            
            return False
            
        except Exception as e:
            logger.error(f"Permission check failed: {e}")
            raise
    
    def get_user_roles(
        self,
        username: str
    ) -> List[str]:
        """
        Get user roles.
        
        Args:
            username: Username
        
        Returns:
            List of roles
        """
        try:
            if username not in self.users:
                raise ValueError(f"User not found: {username}")
            
            return self.users[username]['roles']
            
        except Exception as e:
            logger.error(f"Failed to get user roles: {e}")
            raise
    
    def update_user_roles(
        self,
        username: str,
        roles: List[str]
    ) -> None:
        """
        Update user roles.
        
        Args:
            username: Username
            roles: New roles
        """
        try:
            if username not in self.users:
                raise ValueError(f"User not found: {username}")
            
            # Validate roles
            for role in roles:
                if role not in self.roles:
                    raise ValueError(f"Invalid role: {role}")
            
            self.users[username]['roles'] = roles
            logger.info(f"Updated roles for user: {username}")
            
        except Exception as e:
            logger.error(f"Failed to update user roles: {e}")
            raise 