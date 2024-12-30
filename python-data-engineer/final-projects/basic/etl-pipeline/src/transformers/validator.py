"""
Data validation module.
"""

import logging
from typing import Dict, Any, List
import pandas as pd
from datetime import datetime
import re
from src.utils.config import Config
from src.utils.logging import setup_logger

logger = setup_logger(__name__)

class DataValidator:
    """Data validation management."""
    
    def __init__(self, config: Config):
        """Initialize validator."""
        self.config = config
        self.validation_rules = config.validations
        self.validation_errors = []
    
    def validate_customer_data(self, df: pd.DataFrame) -> bool:
        """
        Validate customer data.
        
        Args:
            df: Customer DataFrame
        
        Returns:
            Whether validation passed
        """
        try:
            self.validation_errors = []
            rules = self.validation_rules['customers']
            
            # Validate email format
            invalid_emails = df[
                ~df['email'].str.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
            ]
            if not invalid_emails.empty:
                self.validation_errors.append({
                    'field': 'email',
                    'error': 'Invalid email format',
                    'records': invalid_emails.index.tolist()
                })
            
            # Validate phone format
            invalid_phones = df[
                ~df['phone'].str.match(r'^\+?[1-9]\d{1,14}$')
            ]
            if not invalid_phones.empty:
                self.validation_errors.append({
                    'field': 'phone',
                    'error': 'Invalid phone format',
                    'records': invalid_phones.index.tolist()
                })
            
            # Check required fields
            for field in ['first_name', 'last_name', 'email']:
                missing = df[df[field].isna()]
                if not missing.empty:
                    self.validation_errors.append({
                        'field': field,
                        'error': 'Required field missing',
                        'records': missing.index.tolist()
                    })
            
            valid = len(self.validation_errors) == 0
            if not valid:
                logger.warning(f"Found {len(self.validation_errors)} validation errors")
            
            return valid
            
        except Exception as e:
            logger.error(f"Failed to validate customer data: {e}")
            raise
    
    def validate_transaction_data(self, df: pd.DataFrame) -> bool:
        """
        Validate transaction data.
        
        Args:
            df: Transaction DataFrame
        
        Returns:
            Whether validation passed
        """
        try:
            self.validation_errors = []
            rules = self.validation_rules['transactions']
            
            # Validate amount
            invalid_amounts = df[
                (df['amount'] <= 0) | (df['amount'].isna())
            ]
            if not invalid_amounts.empty:
                self.validation_errors.append({
                    'field': 'amount',
                    'error': 'Invalid amount',
                    'records': invalid_amounts.index.tolist()
                })
            
            # Validate dates
            invalid_dates = df[
                (df['transaction_date'] > datetime.now()) |
                (df['transaction_date'] < pd.Timestamp('2020-01-01'))
            ]
            if not invalid_dates.empty:
                self.validation_errors.append({
                    'field': 'transaction_date',
                    'error': 'Invalid date',
                    'records': invalid_dates.index.tolist()
                })
            
            # Validate status
            valid_statuses = ['COMPLETED', 'PENDING', 'FAILED']
            invalid_status = df[~df['status'].isin(valid_statuses)]
            if not invalid_status.empty:
                self.validation_errors.append({
                    'field': 'status',
                    'error': 'Invalid status',
                    'records': invalid_status.index.tolist()
                })
            
            valid = len(self.validation_errors) == 0
            if not valid:
                logger.warning(f"Found {len(self.validation_errors)} validation errors")
            
            return valid
            
        except Exception as e:
            logger.error(f"Failed to validate transaction data: {e}")
            raise
    
    def get_validation_errors(self) -> List[Dict[str, Any]]:
        """Get validation errors."""
        return self.validation_errors 