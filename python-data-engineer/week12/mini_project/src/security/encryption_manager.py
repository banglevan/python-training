"""
Data encryption management.
"""

from typing import Dict, Any, Optional, List, Union
import logging
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
import base64
import json
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EncryptionManager:
    """Data encryption management."""
    
    def __init__(
        self,
        master_key: Optional[str] = None,
        key_rotation_days: int = 30
    ):
        """Initialize manager."""
        self.master_key = master_key or self._generate_key()
        self.key_rotation_days = key_rotation_days
        self.column_keys = {}
        self.encrypted_columns = {}
    
    def encrypt_column(
        self,
        table: str,
        column: str,
        data: Union[str, bytes]
    ) -> bytes:
        """
        Encrypt column data.
        
        Args:
            table: Table name
            column: Column name
            data: Data to encrypt
        
        Returns:
            Encrypted data
        """
        try:
            key = self._get_column_key(table, column)
            f = Fernet(key)
            
            # Convert to bytes if string
            if isinstance(data, str):
                data = data.encode()
            
            # Encrypt data
            encrypted = f.encrypt(data)
            
            # Store column metadata
            if table not in self.encrypted_columns:
                self.encrypted_columns[table] = {}
            
            self.encrypted_columns[table][column] = {
                'encrypted': True,
                'key_id': key
            }
            
            return encrypted
            
        except Exception as e:
            logger.error(f"Failed to encrypt data: {e}")
            raise
    
    def decrypt_column(
        self,
        table: str,
        column: str,
        data: bytes
    ) -> bytes:
        """
        Decrypt column data.
        
        Args:
            table: Table name
            column: Column name
            data: Data to decrypt
        
        Returns:
            Decrypted data
        """
        try:
            key = self._get_column_key(table, column)
            f = Fernet(key)
            
            # Decrypt data
            decrypted = f.decrypt(data)
            return decrypted
            
        except Exception as e:
            logger.error(f"Failed to decrypt data: {e}")
            raise
    
    def rotate_keys(self) -> None:
        """Rotate encryption keys."""
        try:
            # Generate new keys
            for table in self.column_keys:
                for column in self.column_keys[table]:
                    new_key = self._generate_key()
                    self.column_keys[table][column] = new_key
            
            logger.info("Rotated encryption keys")
            
        except Exception as e:
            logger.error(f"Failed to rotate keys: {e}")
            raise
    
    def _generate_key(self) -> bytes:
        """Generate encryption key."""
        try:
            salt = os.urandom(16)
            kdf = PBKDF2HMAC(
                algorithm=hashes.SHA256(),
                length=32,
                salt=salt,
                iterations=100000
            )
            
            key = base64.urlsafe_b64encode(
                kdf.derive(self.master_key.encode())
            )
            return key
            
        except Exception as e:
            logger.error(f"Failed to generate key: {e}")
            raise
    
    def _get_column_key(
        self,
        table: str,
        column: str
    ) -> bytes:
        """Get or create column key."""
        try:
            if table not in self.column_keys:
                self.column_keys[table] = {}
            
            if column not in self.column_keys[table]:
                self.column_keys[table][column] = self._generate_key()
            
            return self.column_keys[table][column]
            
        except Exception as e:
            logger.error(f"Failed to get column key: {e}")
            raise
    
    def export_keys(
        self,
        path: str
    ) -> None:
        """
        Export encryption keys.
        
        Args:
            path: Export file path
        """
        try:
            # Convert keys to strings for JSON
            export_data = {
                table: {
                    col: key.decode()
                    for col, key in columns.items()
                }
                for table, columns in self.column_keys.items()
            }
            
            with open(path, 'w') as f:
                json.dump(export_data, f)
            
            logger.info(f"Exported keys to: {path}")
            
        except Exception as e:
            logger.error(f"Failed to export keys: {e}")
            raise
    
    def import_keys(
        self,
        path: str
    ) -> None:
        """
        Import encryption keys.
        
        Args:
            path: Import file path
        """
        try:
            with open(path, 'r') as f:
                import_data = json.load(f)
            
            # Convert strings back to bytes
            self.column_keys = {
                table: {
                    col: key.encode()
                    for col, key in columns.items()
                }
                for table, columns in import_data.items()
            }
            
            logger.info(f"Imported keys from: {path}")
            
        except Exception as e:
            logger.error(f"Failed to import keys: {e}")
            raise 