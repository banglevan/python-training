"""
Tiering Policies
-------------

Defines rules for:
1. Data placement
2. Migration triggers
3. Performance targets
"""

from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

class TieringPolicy:
    """Storage tiering policy definition."""
    
    def __init__(
        self,
        name: str,
        config: Dict[str, Any]
    ):
        """Initialize tiering policy."""
        self.name = name
        self.storage = config['storage']
        self.max_age = self._parse_duration(
            config.get('max_age', '0d')
        )
        self.min_access = config.get('min_access', 0)
        self.max_size = self._parse_size(
            config.get('max_size', '0')
        )
        self.priority = config.get('priority', 0)
        self.conditions = config.get('conditions', {})
        
        logger.info(
            f"Initialized tiering policy: {self.name}"
        )
    
    def evaluate(
        self,
        metadata: Dict[str, Any]
    ) -> bool:
        """Evaluate if data matches policy."""
        try:
            # Check age condition
            if self.max_age > 0:
                created = datetime.fromisoformat(
                    metadata['timestamp']
                )
                age = datetime.now() - created
                if age > self.max_age:
                    return False
            
            # Check access frequency
            if self.min_access > 0:
                access_count = metadata.get(
                    'access_count',
                    0
                )
                if access_count < self.min_access:
                    return False
            
            # Check size condition
            if self.max_size > 0:
                size = metadata['size']
                if size > self.max_size:
                    return False
            
            # Check custom conditions
            for key, value in self.conditions.items():
                if key in metadata:
                    if isinstance(value, (list, tuple)):
                        if metadata[key] not in value:
                            return False
                    elif metadata[key] != value:
                        return False
            
            return True
            
        except Exception as e:
            logger.error(
                f"Policy evaluation failed: {e}"
            )
            return False
    
    def calculate_score(
        self,
        metadata: Dict[str, Any]
    ) -> float:
        """Calculate policy match score."""
        try:
            score = self.priority
            
            # Age score
            if self.max_age > 0:
                created = datetime.fromisoformat(
                    metadata['timestamp']
                )
                age = datetime.now() - created
                age_ratio = age / self.max_age
                score += (1 - age_ratio) * 10
            
            # Access score
            if self.min_access > 0:
                access_count = metadata.get(
                    'access_count',
                    0
                )
                access_ratio = min(
                    access_count / self.min_access,
                    1.0
                )
                score += access_ratio * 10
            
            # Size score
            if self.max_size > 0:
                size = metadata['size']
                size_ratio = size / self.max_size
                score += (1 - size_ratio) * 5
            
            return max(0, score)
            
        except Exception as e:
            logger.error(
                f"Score calculation failed: {e}"
            )
            return 0
    
    def _parse_duration(
        self,
        duration: str
    ) -> timedelta:
        """Parse duration string (e.g., '7d')."""
        try:
            value = int(duration[:-1])
            unit = duration[-1].lower()
            
            if unit == 'd':
                return timedelta(days=value)
            elif unit == 'h':
                return timedelta(hours=value)
            elif unit == 'm':
                return timedelta(minutes=value)
            else:
                return timedelta()
                
        except Exception:
            return timedelta()
    
    def _parse_size(
        self,
        size: str
    ) -> int:
        """Parse size string (e.g., '1GB')."""
        try:
            units = {
                'B': 1,
                'KB': 1024,
                'MB': 1024**2,
                'GB': 1024**3,
                'TB': 1024**4
            }
            
            size = size.strip().upper()
            for unit, multiplier in units.items():
                if size.endswith(unit):
                    value = float(size[:-len(unit)])
                    return int(value * multiplier)
            
            return int(size)
            
        except Exception:
            return 0
    
    def __repr__(self) -> str:
        return (
            f"TieringPolicy("
            f"name='{self.name}', "
            f"storage='{self.storage}', "
            f"max_age={self.max_age}, "
            f"min_access={self.min_access}, "
            f"max_size={self.max_size}, "
            f"priority={self.priority}"
            f")"
        ) 