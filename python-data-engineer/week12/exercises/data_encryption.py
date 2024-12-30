"""
Data encryption exercises.
"""

from typing import Dict, Any, Optional, Tuple
import logging
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
import base64
import os
import json
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataEncryption:
    """Data encryption operations."""
    
    def __init__(self, key_store_path: str = "keys/"):
        """
        Initialize encryption manager.
        
        Args:
            key_store_path: Path to key store
        """
        self.key_store_path = key_store_path
        os.makedirs(key_store_path, exist_ok=True)
        
        # Initialize key rotation schedule
        self.rotation_interval = timedelta(days=90)  # 90 days
        self.current_key_id = None
        self.symmetric_key = None
        self.load_or_generate_keys()
    
    def generate_symmetric_key(self) -> Tuple[str, bytes]:
        """
        Generate symmetric key.
        
        Returns:
            Tuple of key ID and key
        """
        try:
            key = Fernet.generate_key()
            key_id = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Store key
            key_path = os.path.join(self.key_store_path, f"sym_{key_id}.key")
            with open(key_path, 'wb') as f:
                f.write(key)
            
            logger.info(f"Generated symmetric key: {key_id}")
            return key_id, key
            
        except Exception as e:
            logger.error(f"Failed to generate symmetric key: {e}")
            raise
    
    def generate_asymmetric_keys(self) -> Tuple[str, rsa.RSAPrivateKey, rsa.RSAPublicKey]:
        """
        Generate asymmetric key pair.
        
        Returns:
            Tuple of key ID, private key, and public key
        """
        try:
            # Generate private key
            private_key = rsa.generate_private_key(
                public_exponent=65537,
                key_size=2048
            )
            
            # Get public key
            public_key = private_key.public_key()
            
            # Generate key ID
            key_id = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Store private key
            private_path = os.path.join(self.key_store_path, f"private_{key_id}.pem")
            with open(private_path, 'wb') as f:
                f.write(private_key.private_bytes(
                    encoding=serialization.Encoding.PEM,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption()
                ))
            
            # Store public key
            public_path = os.path.join(self.key_store_path, f"public_{key_id}.pem")
            with open(public_path, 'wb') as f:
                f.write(public_key.public_bytes(
                    encoding=serialization.Encoding.PEM,
                    format=serialization.PublicFormat.SubjectPublicKeyInfo
                ))
            
            logger.info(f"Generated asymmetric key pair: {key_id}")
            return key_id, private_key, public_key
            
        except Exception as e:
            logger.error(f"Failed to generate asymmetric keys: {e}")
            raise
    
    def encrypt_symmetric(self, data: bytes) -> Tuple[str, bytes]:
        """
        Encrypt data using symmetric encryption.
        
        Args:
            data: Data to encrypt
        
        Returns:
            Tuple of key ID and encrypted data
        """
        try:
            if not self.symmetric_key:
                self.rotate_symmetric_key()
            
            f = Fernet(self.symmetric_key)
            encrypted = f.encrypt(data)
            
            return self.current_key_id, encrypted
            
        except Exception as e:
            logger.error(f"Failed to encrypt data: {e}")
            raise
    
    def decrypt_symmetric(
        self,
        encrypted_data: bytes,
        key_id: str
    ) -> bytes:
        """
        Decrypt symmetrically encrypted data.
        
        Args:
            encrypted_data: Encrypted data
            key_id: Key ID
        
        Returns:
            Decrypted data
        """
        try:
            # Load key
            key_path = os.path.join(self.key_store_path, f"sym_{key_id}.key")
            with open(key_path, 'rb') as f:
                key = f.read()
            
            f = Fernet(key)
            decrypted = f.decrypt(encrypted_data)
            
            return decrypted
            
        except Exception as e:
            logger.error(f"Failed to decrypt data: {e}")
            raise
    
    def rotate_symmetric_key(self) -> None:
        """Rotate symmetric encryption key."""
        try:
            key_id, key = self.generate_symmetric_key()
            
            # Update current key
            self.current_key_id = key_id
            self.symmetric_key = key
            
            # Store rotation time
            metadata = {
                'last_rotation': datetime.now().isoformat(),
                'current_key_id': key_id
            }
            
            with open(os.path.join(self.key_store_path, 'metadata.json'), 'w') as f:
                json.dump(metadata, f)
            
            logger.info("Rotated symmetric key")
            
        except Exception as e:
            logger.error(f"Failed to rotate key: {e}")
            raise
    
    def load_or_generate_keys(self) -> None:
        """Load existing keys or generate new ones."""
        try:
            metadata_path = os.path.join(self.key_store_path, 'metadata.json')
            
            if os.path.exists(metadata_path):
                with open(metadata_path, 'r') as f:
                    metadata = json.load(f)
                
                last_rotation = datetime.fromisoformat(metadata['last_rotation'])
                
                if datetime.now() - last_rotation > self.rotation_interval:
                    self.rotate_symmetric_key()
                else:
                    self.current_key_id = metadata['current_key_id']
                    key_path = os.path.join(
                        self.key_store_path,
                        f"sym_{self.current_key_id}.key"
                    )
                    with open(key_path, 'rb') as f:
                        self.symmetric_key = f.read()
            else:
                self.rotate_symmetric_key()
            
        except Exception as e:
            logger.error(f"Failed to load keys: {e}")
            raise 