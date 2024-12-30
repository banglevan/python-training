"""
File data extraction module.
"""

import logging
from typing import Dict, Any, List
import pandas as pd
import json
from pathlib import Path
from src.utils.config import Config
from src.utils.logging import setup_logger

logger = setup_logger(__name__)

class FileExtractor:
    """File data extraction management."""
    
    def __init__(self, config: Config):
        """Initialize extractor."""
        self.config = config
    
    def extract_product_data(self) -> pd.DataFrame:
        """
        Extract product data from CSV files.
        
        Returns:
            Product data DataFrame
        """
        try:
            # Get file paths
            pattern = self.config.files['paths']['products']
            files = list(Path().glob(pattern))
            
            if not files:
                logger.warning(f"No product files found: {pattern}")
                return pd.DataFrame()
            
            # Read and combine files
            dfs = []
            for file in files:
                df = pd.read_csv(
                    file,
                    parse_dates=['created_at', 'updated_at']
                )
                dfs.append(df)
            
            # Combine all DataFrames
            combined_df = pd.concat(dfs, ignore_index=True)
            
            logger.info(f"Extracted {len(combined_df)} product records")
            return combined_df
            
        except Exception as e:
            logger.error(f"Failed to extract product data: {e}")
            raise
    
    def extract_user_preferences(self) -> List[Dict[str, Any]]:
        """
        Extract user preferences from JSON files.
        
        Returns:
            List of user preferences
        """
        try:
            # Get file paths
            pattern = self.config.files['paths']['preferences']
            files = list(Path().glob(pattern))
            
            if not files:
                logger.warning(f"No preference files found: {pattern}")
                return []
            
            # Read and combine files
            preferences = []
            for file in files:
                with open(file, 'r') as f:
                    data = json.load(f)
                    preferences.extend(data)
            
            logger.info(f"Extracted {len(preferences)} user preferences")
            return preferences
            
        except Exception as e:
            logger.error(f"Failed to extract user preferences: {e}")
            raise
    
    def extract_sales_data(self) -> pd.DataFrame:
        """
        Extract sales data from Excel files.
        
        Returns:
            Sales data DataFrame
        """
        try:
            # Get file paths
            pattern = self.config.files['paths']['sales']
            files = list(Path().glob(pattern))
            
            if not files:
                logger.warning(f"No sales files found: {pattern}")
                return pd.DataFrame()
            
            # Read and combine files
            dfs = []
            for file in files:
                df = pd.read_excel(
                    file,
                    parse_dates=['sale_date']
                )
                dfs.append(df)
            
            # Combine all DataFrames
            combined_df = pd.concat(dfs, ignore_index=True)
            
            logger.info(f"Extracted {len(combined_df)} sales records")
            return combined_df
            
        except Exception as e:
            logger.error(f"Failed to extract sales data: {e}")
            raise 