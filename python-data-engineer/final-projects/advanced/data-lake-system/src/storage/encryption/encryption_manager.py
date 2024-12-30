"""
Column-level encryption management.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from typing import List, Dict, Any
import base64
from cryptography.fernet import Fernet
import logging
import os

logger = logging.getLogger(__name__)

class EncryptionManager:
    """Manage column-level encryption."""
    
    def __init__(self):
        """Initialize encryption manager."""
        self.key = os.getenv(
            'ENCRYPTION_KEY',
            Fernet.generate_key()
        )
        self.fernet = Fernet(self.key)
    
    def encrypt_columns(
        self,
        df: DataFrame,
        columns: List[str]
    ) -> DataFrame:
        """
        Encrypt specified columns.
        
        Args:
            df: Source DataFrame
            columns: Columns to encrypt
            
        Returns:
            DataFrame with encrypted columns
        """
        try:
            @udf(returnType=StringType())
            def encrypt(value):
                if value is None:
                    return None
                return base64.b64encode(
                    self.fernet.encrypt(
                        str(value).encode()
                    )
                ).decode()
            
            for column in columns:
                if column in df.columns:
                    df = df.withColumn(
                        column,
                        encrypt(df[column])
                    )
            
            return df
            
        except Exception as e:
            logger.error(f"Encryption failed: {e}")
            raise
    
    def decrypt_columns(
        self,
        df: DataFrame,
        columns: List[str]
    ) -> DataFrame:
        """
        Decrypt specified columns.
        
        Args:
            df: Source DataFrame
            columns: Columns to decrypt
            
        Returns:
            DataFrame with decrypted columns
        """
        try:
            @udf(returnType=StringType())
            def decrypt(value):
                if value is None:
                    return None
                return self.fernet.decrypt(
                    base64.b64decode(value)
                ).decode()
            
            for column in columns:
                if column in df.columns:
                    df = df.withColumn(
                        column,
                        decrypt(df[column])
                    )
            
            return df
            
        except Exception as e:
            logger.error(f"Decryption failed: {e}")
            raise
    
    def rotate_key(self) -> None:
        """Rotate encryption key."""
        try:
            new_key = Fernet.generate_key()
            new_fernet = Fernet(new_key)
            
            # TODO: Implement key rotation logic
            # This would involve:
            # 1. Reading all encrypted data
            # 2. Decrypting with old key
            # 3. Encrypting with new key
            # 4. Writing back to storage
            
            self.key = new_key
            self.fernet = new_fernet
            
        except Exception as e:
            logger.error(f"Key rotation failed: {e}")
            raise 