"""
Backup System
-----------

Manages data backup operations with:
1. Multiple strategies
2. Scheduling
3. Retention policies
4. Recovery procedures
"""

from .strategy import BackupStrategy
from .scheduler import BackupScheduler
from .manager import BackupManager

__all__ = [
    'BackupStrategy',
    'BackupScheduler',
    'BackupManager'
] 