"""
Storage Tiering
-------------

Manages data placement across storage tiers based on:
1. Access patterns
2. Age of data
3. Performance requirements
4. Cost optimization
"""

from .policy import TieringPolicy
from .manager import TieringManager

__all__ = ['TieringPolicy', 'TieringManager'] 