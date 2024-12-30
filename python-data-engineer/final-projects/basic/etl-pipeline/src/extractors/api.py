"""
API data extraction module.
"""

import logging
import requests
from typing import Dict, Any, Optional
from datetime import datetime
from ratelimit import limits, sleep_and_retry
from src.utils.config import Config
from src.utils.logging import setup_logger

logger = setup_logger(__name__)

class APIExtractor:
    """API data extraction management."""
    
    def __init__(self, config: Config):
        """Initialize extractor."""
        self.config = config
        self.session = requests.Session()
        self.headers = {
            'User-Agent': 'ETL-Pipeline/1.0'
        }
    
    @sleep_and_retry
    @limits(calls=100, period=60)  # Rate limit: 100 calls per minute
    def extract_weather_data(
        self,
        city: str,
        country_code: str
    ) -> Dict[str, Any]:
        """
        Extract weather data.
        
        Args:
            city: City name
            country_code: Country code
        
        Returns:
            Weather data dictionary
        """
        try:
            # Build URL
            url = f"{self.config.apis['weather']['url']}/weather"
            params = {
                'q': f"{city},{country_code}",
                'appid': self.config.apis['weather']['key'],
                'units': 'metric'
            }
            
            # Make request
            response = self.session.get(
                url,
                params=params,
                headers=self.headers
            )
            response.raise_for_status()
            
            # Parse response
            data = response.json()
            
            # Transform to standard format
            weather_data = {
                'city': city,
                'country': country_code,
                'timestamp': datetime.now().isoformat(),
                'temperature': data['main']['temp'],
                'humidity': data['main']['humidity'],
                'pressure': data['main']['pressure'],
                'description': data['weather'][0]['description']
            }
            
            logger.info(f"Extracted weather data for {city}")
            return weather_data
            
        except Exception as e:
            logger.error(f"Failed to extract weather data: {e}")
            raise
    
    @sleep_and_retry
    @limits(calls=5, period=60)  # Rate limit: 5 calls per minute
    def extract_stock_data(
        self,
        symbol: str
    ) -> Dict[str, Any]:
        """
        Extract stock market data.
        
        Args:
            symbol: Stock symbol
        
        Returns:
            Stock data dictionary
        """
        try:
            # Build URL
            url = self.config.apis['stocks']['url']
            params = {
                'function': 'GLOBAL_QUOTE',
                'symbol': symbol,
                'apikey': self.config.apis['stocks']['key']
            }
            
            # Make request
            response = self.session.get(
                url,
                params=params,
                headers=self.headers
            )
            response.raise_for_status()
            
            # Parse response
            data = response.json()
            quote = data['Global Quote']
            
            # Transform to standard format
            stock_data = {
                'symbol': symbol,
                'timestamp': datetime.now().isoformat(),
                'price': float(quote['05. price']),
                'volume': int(quote['06. volume']),
                'change_percent': float(quote['10. change percent'].rstrip('%'))
            }
            
            logger.info(f"Extracted stock data for {symbol}")
            return stock_data
            
        except Exception as e:
            logger.error(f"Failed to extract stock data: {e}")
            raise
    
    def close(self):
        """Close session."""
        self.session.close() 