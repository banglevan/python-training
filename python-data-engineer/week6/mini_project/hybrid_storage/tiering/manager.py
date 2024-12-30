"""
Tiering Manager
------------

Coordinates data movement between tiers based on:
1. Policy evaluation
2. Storage capacity
3. Access patterns
4. Performance metrics
"""

from typing import Dict, Any, Optional, List, Union
from pathlib import Path
import logging
import json
from datetime import datetime
import asyncio
from concurrent.futures import ThreadPoolExecutor
import sqlite3
import threading

from .policy import TieringPolicy

logger = logging.getLogger(__name__)

class TieringManager:
    """Storage tiering coordinator."""
    
    def __init__(
        self,
        config: Dict[str, Any],
        storage_backends: Dict[str, Any]
    ):
        """Initialize tiering manager."""
        self.storage = storage_backends
        self.policies = self._init_policies(
            config['policies']
        )
        
        # Initialize database
        self.db_path = Path('tiering.db')
        self._init_database()
        
        # Initialize thread pool
        self.executor = ThreadPoolExecutor(
            max_workers=config.get('threads', 4)
        )
        
        # Initialize lock for thread safety
        self.lock = threading.Lock()
        
        logger.info("Initialized TieringManager")
    
    def determine_tier(
        self,
        file_path: Union[str, Path],
        metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """Determine optimal storage tier."""
        try:
            file_path = Path(file_path)
            
            # Get or create metadata
            if metadata is None:
                metadata = {
                    'path': str(file_path),
                    'size': file_path.stat().st_size,
                    'timestamp': datetime.now().isoformat(),
                    'access_count': 0
                }
            
            # Evaluate policies
            best_tier = None
            best_score = -1
            
            for name, policy in self.policies.items():
                if policy.evaluate(metadata):
                    score = policy.calculate_score(metadata)
                    if score > best_score:
                        best_tier = name
                        best_score = score
            
            # Default to first tier if no match
            if best_tier is None:
                best_tier = next(iter(self.policies))
            
            logger.info(
                f"Selected tier '{best_tier}' for {file_path}"
            )
            return best_tier
            
        except Exception as e:
            logger.error(f"Tier determination failed: {e}")
            raise
    
    async def check_policies(self):
        """Check and enforce tiering policies."""
        try:
            # Get all file metadata
            files = self.get_all_metadata()
            
            moves = []
            for file_info in files:
                current_tier = file_info['current_tier']
                optimal_tier = self.determine_tier(
                    file_info['path'],
                    file_info
                )
                
                if current_tier != optimal_tier:
                    moves.append({
                        'path': file_info['path'],
                        'from_tier': current_tier,
                        'to_tier': optimal_tier,
                        'metadata': file_info
                    })
            
            # Execute moves in parallel
            if moves:
                await asyncio.gather(*[
                    self._move_file(move)
                    for move in moves
                ])
            
        except Exception as e:
            logger.error(f"Policy check failed: {e}")
            raise
    
    def update_metadata(
        self,
        file_path: Union[str, Path],
        tier: str,
        storage_info: Dict[str, Any]
    ):
        """Update file metadata."""
        try:
            with self.lock:
                with sqlite3.connect(self.db_path) as conn:
                    cursor = conn.cursor()
                    
                    # Update or insert metadata
                    cursor.execute(
                        """
                        INSERT OR REPLACE INTO file_metadata
                        (path, current_tier, storage_path,
                         size, timestamp, metadata)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (
                            str(file_path),
                            tier,
                            storage_info['path'],
                            storage_info['size'],
                            datetime.now().isoformat(),
                            json.dumps(storage_info['metadata'])
                        )
                    )
            
        except Exception as e:
            logger.error(f"Metadata update failed: {e}")
            raise
    
    def update_access(
        self,
        file_path: Union[str, Path],
        tier: str
    ):
        """Update file access information."""
        try:
            with self.lock:
                with sqlite3.connect(self.db_path) as conn:
                    cursor = conn.cursor()
                    
                    # Update access count and timestamp
                    cursor.execute(
                        """
                        UPDATE file_metadata
                        SET access_count = access_count + 1,
                            last_access = ?
                        WHERE path = ?
                        """,
                        (
                            datetime.now().isoformat(),
                            str(file_path)
                        )
                    )
            
        except Exception as e:
            logger.error(f"Access update failed: {e}")
            raise
    
    def get_file_location(
        self,
        file_path: Union[str, Path],
        version: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """Get current file location."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute(
                    """
                    SELECT current_tier, storage_path, metadata
                    FROM file_metadata
                    WHERE path = ?
                    """,
                    (str(file_path),)
                )
                
                row = cursor.fetchone()
                if row:
                    return {
                        'tier': row[0],
                        'path': row[1],
                        'metadata': json.loads(row[2])
                    }
                return None
            
        except Exception as e:
            logger.error(f"Location lookup failed: {e}")
            raise
    
    def remove_metadata(
        self,
        file_path: Union[str, Path],
        version: Optional[str] = None
    ):
        """Remove file metadata."""
        try:
            with self.lock:
                with sqlite3.connect(self.db_path) as conn:
                    cursor = conn.cursor()
                    
                    cursor.execute(
                        """
                        DELETE FROM file_metadata
                        WHERE path = ?
                        """,
                        (str(file_path),)
                    )
            
        except Exception as e:
            logger.error(f"Metadata removal failed: {e}")
            raise
    
    def get_all_metadata(self) -> List[Dict[str, Any]]:
        """Get metadata for all files."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute(
                    """
                    SELECT path, current_tier, storage_path,
                           size, timestamp, metadata,
                           access_count, last_access
                    FROM file_metadata
                    """
                )
                
                return [
                    {
                        'path': row[0],
                        'current_tier': row[1],
                        'storage_path': row[2],
                        'size': row[3],
                        'timestamp': row[4],
                        'metadata': json.loads(row[5]),
                        'access_count': row[6],
                        'last_access': row[7]
                    }
                    for row in cursor.fetchall()
                ]
            
        except Exception as e:
            logger.error(f"Metadata retrieval failed: {e}")
            raise
    
    async def _move_file(
        self,
        move_info: Dict[str, Any]
    ):
        """Move file between tiers."""
        try:
            path = move_info['path']
            from_tier = move_info['from_tier']
            to_tier = move_info['to_tier']
            
            logger.info(
                f"Moving {path} from {from_tier} to {to_tier}"
            )
            
            # Get source and destination storage
            src_storage = self.storage[
                self.policies[from_tier].storage
            ]
            dst_storage = self.storage[
                self.policies[to_tier].storage
            ]
            
            # Retrieve from source
            src_info = await asyncio.to_thread(
                src_storage.retrieve,
                path
            )
            
            # Store in destination
            dst_info = await asyncio.to_thread(
                dst_storage.store,
                src_info['path'],
                src_info['metadata']
            )
            
            # Update metadata
            self.update_metadata(
                path,
                to_tier,
                dst_info
            )
            
            # Delete from source
            await asyncio.to_thread(
                src_storage.delete,
                path
            )
            
        except Exception as e:
            logger.error(f"File move failed: {e}")
            raise
    
    def _init_policies(
        self,
        config: Dict[str, Any]
    ) -> Dict[str, TieringPolicy]:
        """Initialize tiering policies."""
        return {
            name: TieringPolicy(name, policy_config)
            for name, policy_config in config.items()
        }
    
    def _init_database(self):
        """Initialize SQLite database."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Create metadata table
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS file_metadata (
                        path TEXT PRIMARY KEY,
                        current_tier TEXT NOT NULL,
                        storage_path TEXT NOT NULL,
                        size INTEGER NOT NULL,
                        timestamp TEXT NOT NULL,
                        metadata TEXT NOT NULL,
                        access_count INTEGER DEFAULT 0,
                        last_access TEXT
                    )
                    """
                )
                
                # Create indexes
                cursor.execute(
                    """
                    CREATE INDEX IF NOT EXISTS
                    idx_current_tier ON file_metadata(current_tier)
                    """
                )
                cursor.execute(
                    """
                    CREATE INDEX IF NOT EXISTS
                    idx_timestamp ON file_metadata(timestamp)
                    """
                )
            
        except Exception as e:
            logger.error(f"Database initialization failed: {e}")
            raise
    
    def close(self):
        """Clean up resources."""
        self.executor.shutdown() 