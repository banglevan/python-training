"""
Data Security
-----------

PURPOSE:
Implement data security features with:
1. Encryption
   - Symmetric/Asymmetric
   - Key management
   - File/Stream encryption

2. Access Control
   - Role-based access
   - Permission management
   - Token validation

3. Audit Logging
   - Activity tracking
   - Security events
   - Compliance reporting
"""

import logging
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
import base64
import jwt
from typing import Dict, List, Any, Optional, Union, BinaryIO
from pathlib import Path
import json
from datetime import datetime, timedelta
import os
import hashlib
import sqlite3
from functools import wraps
import threading
import queue

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataSecurity:
    """Data security handler."""
    
    def __init__(
        self,
        work_dir: Union[str, Path],
        jwt_secret: str,
        db_path: Optional[str] = None
    ):
        """Initialize security system."""
        self.work_dir = Path(work_dir)
        self.work_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize encryption
        self.key_store = self.work_dir / 'key_store'
        self.key_store.mkdir(exist_ok=True)
        
        # Initialize JWT
        self.jwt_secret = jwt_secret
        
        # Initialize database
        self.db_path = db_path or str(
            self.work_dir / 'security.db'
        )
        self._init_database()
        
        # Initialize audit logging
        self.audit_queue = queue.Queue()
        self._start_audit_worker()
        
        logger.info(f"Initialized DataSecurity in {self.work_dir}")
    
    def encrypt_file(
        self,
        file_path: Union[str, Path],
        key_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Encrypt file using symmetric encryption."""
        try:
            file_path = Path(file_path)
            if not file_path.exists():
                raise FileNotFoundError(
                    f"File not found: {file_path}"
                )
            
            # Generate or retrieve key
            key_id = key_id or self._generate_key_id()
            key = self._get_or_create_key(key_id)
            
            # Create Fernet cipher
            cipher = Fernet(key)
            
            # Read and encrypt file
            with open(file_path, 'rb') as f:
                data = f.read()
            
            encrypted_data = cipher.encrypt(data)
            
            # Save encrypted file
            encrypted_path = file_path.with_suffix('.encrypted')
            with open(encrypted_path, 'wb') as f:
                f.write(encrypted_data)
            
            # Store metadata
            if metadata:
                self._store_encryption_metadata(
                    key_id,
                    str(file_path),
                    metadata
                )
            
            result = {
                'key_id': key_id,
                'original_path': str(file_path),
                'encrypted_path': str(encrypted_path),
                'size': len(encrypted_data)
            }
            
            # Log encryption event
            self.log_audit_event(
                'encrypt_file',
                result
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Encryption error: {e}")
            raise
    
    def decrypt_file(
        self,
        file_path: Union[str, Path],
        key_id: str,
        output_path: Optional[Union[str, Path]] = None
    ) -> Dict[str, Any]:
        """Decrypt file using symmetric encryption."""
        try:
            file_path = Path(file_path)
            if not file_path.exists():
                raise FileNotFoundError(
                    f"File not found: {file_path}"
                )
            
            # Retrieve key
            key = self._get_key(key_id)
            if not key:
                raise ValueError(f"Key not found: {key_id}")
            
            # Create Fernet cipher
            cipher = Fernet(key)
            
            # Read and decrypt file
            with open(file_path, 'rb') as f:
                encrypted_data = f.read()
            
            decrypted_data = cipher.decrypt(encrypted_data)
            
            # Save decrypted file
            output_path = output_path or file_path.with_suffix('')
            with open(output_path, 'wb') as f:
                f.write(decrypted_data)
            
            result = {
                'key_id': key_id,
                'encrypted_path': str(file_path),
                'decrypted_path': str(output_path),
                'size': len(decrypted_data)
            }
            
            # Log decryption event
            self.log_audit_event(
                'decrypt_file',
                result
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Decryption error: {e}")
            raise
    
    def create_user(
        self,
        username: str,
        password: str,
        roles: List[str]
    ) -> Dict[str, Any]:
        """Create new user with roles."""
        try:
            # Hash password
            password_hash = self._hash_password(password)
            
            # Store user in database
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Insert user
                cursor.execute(
                    """
                    INSERT INTO users (username, password_hash)
                    VALUES (?, ?)
                    """,
                    (username, password_hash)
                )
                user_id = cursor.lastrowid
                
                # Assign roles
                for role in roles:
                    cursor.execute(
                        """
                        INSERT INTO user_roles (user_id, role)
                        VALUES (?, ?)
                        """,
                        (user_id, role)
                    )
            
            result = {
                'user_id': user_id,
                'username': username,
                'roles': roles
            }
            
            # Log user creation
            self.log_audit_event(
                'create_user',
                result
            )
            
            return result
            
        except Exception as e:
            logger.error(f"User creation error: {e}")
            raise
    
    def authenticate(
        self,
        username: str,
        password: str
    ) -> Optional[str]:
        """Authenticate user and return JWT token."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Get user
                cursor.execute(
                    """
                    SELECT id, password_hash
                    FROM users
                    WHERE username = ?
                    """,
                    (username,)
                )
                result = cursor.fetchone()
                
                if not result:
                    return None
                
                user_id, stored_hash = result
                
                # Verify password
                if not self._verify_password(
                    password,
                    stored_hash
                ):
                    return None
                
                # Get user roles
                cursor.execute(
                    """
                    SELECT role FROM user_roles
                    WHERE user_id = ?
                    """,
                    (user_id,)
                )
                roles = [r[0] for r in cursor.fetchall()]
                
                # Generate token
                token = self._generate_token(
                    user_id,
                    username,
                    roles
                )
                
                # Log authentication
                self.log_audit_event(
                    'authenticate',
                    {'user_id': user_id, 'username': username}
                )
                
                return token
                
        except Exception as e:
            logger.error(f"Authentication error: {e}")
            raise
    
    def verify_token(
        self,
        token: str
    ) -> Optional[Dict[str, Any]]:
        """Verify JWT token and return payload."""
        try:
            payload = jwt.decode(
                token,
                self.jwt_secret,
                algorithms=['HS256']
            )
            return payload
            
        except jwt.InvalidTokenError as e:
            logger.warning(f"Invalid token: {e}")
            return None
    
    def check_permission(
        self,
        token: str,
        required_roles: List[str]
    ) -> bool:
        """Check if token has required roles."""
        payload = self.verify_token(token)
        if not payload:
            return False
        
        user_roles = set(payload.get('roles', []))
        return bool(user_roles & set(required_roles))
    
    def log_audit_event(
        self,
        event_type: str,
        event_data: Dict[str, Any]
    ):
        """Log security audit event."""
        try:
            event = {
                'timestamp': datetime.utcnow().isoformat(),
                'event_type': event_type,
                'event_data': event_data
            }
            
            # Add to queue for async processing
            self.audit_queue.put(event)
            
        except Exception as e:
            logger.error(f"Audit logging error: {e}")
    
    def get_audit_logs(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        event_type: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Retrieve audit logs with filtering."""
        try:
            query = "SELECT * FROM audit_logs WHERE 1=1"
            params = []
            
            if start_time:
                query += " AND timestamp >= ?"
                params.append(start_time.isoformat())
            
            if end_time:
                query += " AND timestamp <= ?"
                params.append(end_time.isoformat())
            
            if event_type:
                query += " AND event_type = ?"
                params.append(event_type)
            
            query += " ORDER BY timestamp DESC LIMIT ?"
            params.append(limit)
            
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                cursor.execute(query, params)
                rows = cursor.fetchall()
                
                return [
                    {
                        'id': row['id'],
                        'timestamp': row['timestamp'],
                        'event_type': row['event_type'],
                        'event_data': json.loads(row['event_data'])
                    }
                    for row in rows
                ]
                
        except Exception as e:
            logger.error(f"Audit log retrieval error: {e}")
            raise
    
    def _init_database(self):
        """Initialize SQLite database."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Create tables
            cursor.executescript("""
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY,
                    username TEXT UNIQUE,
                    password_hash TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE TABLE IF NOT EXISTS user_roles (
                    user_id INTEGER,
                    role TEXT,
                    FOREIGN KEY (user_id) REFERENCES users(id),
                    UNIQUE (user_id, role)
                );
                
                CREATE TABLE IF NOT EXISTS audit_logs (
                    id INTEGER PRIMARY KEY,
                    timestamp TEXT,
                    event_type TEXT,
                    event_data TEXT
                );
                
                CREATE TABLE IF NOT EXISTS encryption_metadata (
                    key_id TEXT,
                    file_path TEXT,
                    metadata TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (key_id, file_path)
                );
            """)
    
    def _generate_key_id(self) -> str:
        """Generate unique key ID."""
        return base64.urlsafe_b64encode(
            os.urandom(16)
        ).decode()
    
    def _get_or_create_key(
        self,
        key_id: str
    ) -> bytes:
        """Get existing key or create new one."""
        key_path = self.key_store / key_id
        
        if key_path.exists():
            with open(key_path, 'rb') as f:
                return f.read()
        
        # Generate new key
        key = Fernet.generate_key()
        with open(key_path, 'wb') as f:
            f.write(key)
        
        return key
    
    def _get_key(
        self,
        key_id: str
    ) -> Optional[bytes]:
        """Retrieve existing key."""
        key_path = self.key_store / key_id
        
        if key_path.exists():
            with open(key_path, 'rb') as f:
                return f.read()
        
        return None
    
    def _hash_password(
        self,
        password: str
    ) -> str:
        """Hash password with salt."""
        salt = os.urandom(16)
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=100000
        )
        key = kdf.derive(password.encode())
        
        # Combine salt and key
        return base64.b64encode(
            salt + key
        ).decode()
    
    def _verify_password(
        self,
        password: str,
        stored_hash: str
    ) -> bool:
        """Verify password against stored hash."""
        try:
            # Decode stored hash
            decoded = base64.b64decode(stored_hash)
            salt, stored_key = decoded[:16], decoded[16:]
            
            # Generate key with same salt
            kdf = PBKDF2HMAC(
                algorithm=hashes.SHA256(),
                length=32,
                salt=salt,
                iterations=100000
            )
            key = kdf.derive(password.encode())
            
            return key == stored_key
            
        except Exception:
            return False
    
    def _generate_token(
        self,
        user_id: int,
        username: str,
        roles: List[str]
    ) -> str:
        """Generate JWT token."""
        payload = {
            'user_id': user_id,
            'username': username,
            'roles': roles,
            'exp': datetime.utcnow() + timedelta(hours=24)
        }
        
        return jwt.encode(
            payload,
            self.jwt_secret,
            algorithm='HS256'
        )
    
    def _store_encryption_metadata(
        self,
        key_id: str,
        file_path: str,
        metadata: Dict[str, Any]
    ):
        """Store encryption metadata."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            cursor.execute(
                """
                INSERT OR REPLACE INTO encryption_metadata
                (key_id, file_path, metadata)
                VALUES (?, ?, ?)
                """,
                (
                    key_id,
                    file_path,
                    json.dumps(metadata)
                )
            )
    
    def _start_audit_worker(self):
        """Start background worker for audit logging."""
        def worker():
            while True:
                try:
                    # Get event from queue
                    event = self.audit_queue.get()
                    
                    # Store in database
                    with sqlite3.connect(self.db_path) as conn:
                        cursor = conn.cursor()
                        cursor.execute(
                            """
                            INSERT INTO audit_logs
                            (timestamp, event_type, event_data)
                            VALUES (?, ?, ?)
                            """,
                            (
                                event['timestamp'],
                                event['event_type'],
                                json.dumps(event['event_data'])
                            )
                        )
                    
                    self.audit_queue.task_done()
                    
                except Exception as e:
                    logger.error(f"Audit worker error: {e}")
        
        # Start worker thread
        thread = threading.Thread(
            target=worker,
            daemon=True
        )
        thread.start()

def main():
    """Example usage."""
    security = DataSecurity(
        'security_work',
        jwt_secret='your-secret-key'
    )
    
    try:
        # Create user
        user = security.create_user(
            'testuser',
            'password123',
            ['admin', 'user']
        )
        print(f"Created user: {json.dumps(user, indent=2)}")
        
        # Authenticate
        token = security.authenticate(
            'testuser',
            'password123'
        )
        print(f"JWT token: {token}")
        
        # Encrypt file
        result = security.encrypt_file(
            'sensitive_data.txt',
            metadata={'purpose': 'testing'}
        )
        print(f"Encryption result: {json.dumps(result, indent=2)}")
        
        # Get audit logs
        logs = security.get_audit_logs(
            event_type='encrypt_file',
            limit=5
        )
        print(f"Audit logs: {json.dumps(logs, indent=2)}")
        
    except Exception as e:
        logger.error(f"Error in main: {e}")
        raise

if __name__ == '__main__':
    main() 