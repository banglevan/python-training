"""
Data cleaning module.
"""

import logging
from typing import Dict, Any
import pandas as pd
import numpy as np
from datetime import datetime
from src.utils.config import Config
from src.utils.logging import setup_logger

logger = setup_logger(__name__)

class DataCleaner:
    """Data cleaning management."""
    
    def __init__(self, config: Config):
        """Initialize cleaner."""
        self.config = config
    
    def clean_customer_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean customer data.
        
        Args:
            df: Customer DataFrame
        
        Returns:
            Cleaned DataFrame
        """
        try:
            # Make copy
            cleaned = df.copy()
            
            # Remove duplicates
            cleaned = cleaned.drop_duplicates(subset=['email'], keep='last')
            
            # Clean email addresses
            cleaned['email'] = cleaned['email'].str.lower().str.strip()
            
            # Clean phone numbers
            cleaned['phone'] = cleaned['phone'].apply(self._standardize_phone)
            
            # Fill missing values
            cleaned['address'] = cleaned['address'].fillna('Unknown')
            
            # Convert dates to standard format
            for col in ['created_at', 'updated_at']:
                cleaned[col] = pd.to_datetime(cleaned[col])
            
            logger.info(f"Cleaned {len(cleaned)} customer records")
            return cleaned
            
        except Exception as e:
            logger.error(f"Failed to clean customer data: {e}")
            raise
    
    def clean_transaction_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean transaction data.
        
        Args:
            df: Transaction DataFrame
        
        Returns:
            Cleaned DataFrame
        """
        try:
            # Make copy
            cleaned = df.copy()
            
            # Remove invalid amounts
            cleaned = cleaned[cleaned['amount'] > 0]
            
            # Convert dates to standard format
            cleaned['transaction_date'] = pd.to_datetime(cleaned['transaction_date'])
            
            # Standardize status values
            cleaned['status'] = cleaned['status'].str.upper()
            
            # Remove future dates
            cleaned = cleaned[
                cleaned['transaction_date'] <= datetime.now()
            ]
            
            logger.info(f"Cleaned {len(cleaned)} transaction records")
            return cleaned
            
        except Exception as e:
            logger.error(f"Failed to clean transaction data: {e}")
            raise
    
    def clean_product_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean product data.
        
        Args:
            df: Product DataFrame
        
        Returns:
            Cleaned DataFrame
        """
        try:
            # Make copy
            cleaned = df.copy()
            
            # Remove duplicates
            cleaned = cleaned.drop_duplicates(
                subset=['product_code'],
                keep='last'
            )
            
            # Clean prices
            cleaned['price'] = cleaned['price'].apply(
                lambda x: float(str(x).replace('$', '').strip())
            )
            
            # Remove invalid prices
            cleaned = cleaned[cleaned['price'] >= 0]
            
            # Standardize categories
            cleaned['category'] = cleaned['category'].str.title()
            
            logger.info(f"Cleaned {len(cleaned)} product records")
            return cleaned
            
        except Exception as e:
            logger.error(f"Failed to clean product data: {e}")
            raise
    
    @staticmethod
    def _standardize_phone(phone: str) -> str:
        """Standardize phone number format."""
        if pd.isna(phone):
            return None
            
        # Remove non-numeric characters
        digits = ''.join(filter(str.isdigit, str(phone)))
        
        # Format number
        if len(digits) == 10:
            return f"+1{digits}"
        elif len(digits) == 11 and digits.startswith('1'):
            return f"+{digits}"
        else:
            return digits 