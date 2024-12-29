"""
HDFS Operations
-------------

PURPOSE:
Implement Hadoop Distributed File System operations.

FEATURES:
1. File Management
   - Upload/Download
   - Read/Write/Append
   - Delete/Move/Copy

2. Block Operations
   - Block location
   - Block replication
   - Block verification

3. Cluster Management
   - Datanode status
   - Space monitoring
   - Health checks
"""

from typing import Dict, List, Any, Optional, Union
import logging
from pathlib import Path
import pyarrow as pa
import pyarrow.hdfs as hdfs
import pandas as pd
from datetime import datetime
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class HDFSOperations:
    """HDFS operations handler."""
    
    def __init__(
        self,
        host: str = 'localhost',
        port: int = 9000,
        user: str = None,
        kerb_ticket: str = None,
        extra_conf: Dict[str, str] = None
    ):
        """Initialize HDFS connection."""
        try:
            self.conn = hdfs.connect(
                host=host,
                port=port,
                user=user,
                kerb_ticket=kerb_ticket,
                extra_conf=extra_conf or {}
            )
            logger.info(f"Connected to HDFS: {host}:{port}")
            
        except Exception as e:
            logger.error(f"HDFS connection error: {e}")
            raise
    
    def upload_file(
        self,
        local_path: Union[str, Path],
        hdfs_path: str,
        chunk_size: int = 2**20,  # 1MB
        replication: int = None
    ) -> bool:
        """Upload file to HDFS."""
        try:
            local_path = Path(local_path)
            if not local_path.exists():
                raise FileNotFoundError(
                    f"Local file not found: {local_path}"
                )
            
            # Create parent directories if needed
            self.makedirs(str(Path(hdfs_path).parent))
            
            # Upload file with specified replication
            with local_path.open('rb') as local_file:
                with self.conn.open(
                    hdfs_path,
                    'wb',
                    replication=replication
                ) as hdfs_file:
                    while True:
                        chunk = local_file.read(chunk_size)
                        if not chunk:
                            break
                        hdfs_file.write(chunk)
            
            logger.info(
                f"Uploaded {local_path} to HDFS: {hdfs_path}"
            )
            return True
            
        except Exception as e:
            logger.error(f"Upload error: {e}")
            raise
    
    def download_file(
        self,
        hdfs_path: str,
        local_path: Union[str, Path],
        chunk_size: int = 2**20  # 1MB
    ) -> bool:
        """Download file from HDFS."""
        try:
            local_path = Path(local_path)
            local_path.parent.mkdir(parents=True, exist_ok=True)
            
            with self.conn.open(hdfs_path, 'rb') as hdfs_file:
                with local_path.open('wb') as local_file:
                    while True:
                        chunk = hdfs_file.read(chunk_size)
                        if not chunk:
                            break
                        local_file.write(chunk)
            
            logger.info(
                f"Downloaded {hdfs_path} to: {local_path}"
            )
            return True
            
        except Exception as e:
            logger.error(f"Download error: {e}")
            raise
    
    def read_file(
        self,
        hdfs_path: str,
        encoding: str = 'utf-8'
    ) -> str:
        """Read file content from HDFS."""
        try:
            with self.conn.open(hdfs_path, 'rb') as f:
                content = f.read().decode(encoding)
            return content
            
        except Exception as e:
            logger.error(f"Read error: {e}")
            raise
    
    def write_file(
        self,
        hdfs_path: str,
        content: Union[str, bytes],
        encoding: str = 'utf-8'
    ) -> bool:
        """Write content to HDFS file."""
        try:
            if isinstance(content, str):
                content = content.encode(encoding)
            
            with self.conn.open(hdfs_path, 'wb') as f:
                f.write(content)
            
            logger.info(f"Wrote to HDFS file: {hdfs_path}")
            return True
            
        except Exception as e:
            logger.error(f"Write error: {e}")
            raise
    
    def delete(
        self,
        hdfs_path: str,
        recursive: bool = False
    ) -> bool:
        """Delete file or directory from HDFS."""
        try:
            self.conn.delete(hdfs_path, recursive=recursive)
            logger.info(f"Deleted from HDFS: {hdfs_path}")
            return True
            
        except Exception as e:
            logger.error(f"Delete error: {e}")
            raise
    
    def makedirs(
        self,
        hdfs_path: str,
        permission: int = 0o755
    ) -> bool:
        """Create directory and parents in HDFS."""
        try:
            self.conn.mkdir(hdfs_path, permission=permission)
            logger.info(f"Created HDFS directory: {hdfs_path}")
            return True
            
        except Exception as e:
            logger.error(f"Directory creation error: {e}")
            raise
    
    def list_directory(
        self,
        hdfs_path: str
    ) -> List[Dict[str, Any]]:
        """List directory contents with details."""
        try:
            info = self.conn.ls(hdfs_path, detail=True)
            return [
                {
                    'name': item['name'],
                    'type': item['kind'],
                    'size': item['size'],
                    'modified': datetime.fromtimestamp(
                        item['mtime']
                    ),
                    'owner': item['owner'],
                    'group': item['group'],
                    'permissions': item['mode'],
                    'replication': item.get('replication', None),
                    'block_size': item.get('block_size', None)
                }
                for item in info
            ]
            
        except Exception as e:
            logger.error(f"List directory error: {e}")
            raise
    
    def get_block_locations(
        self,
        hdfs_path: str
    ) -> List[Dict[str, Any]]:
        """Get block locations for a file."""
        try:
            info = self.conn.get_block_locations(hdfs_path)
            return [
                {
                    'offset': block['offset'],
                    'length': block['length'],
                    'hosts': block['hosts']
                }
                for block in info
            ]
            
        except Exception as e:
            logger.error(f"Block location error: {e}")
            raise
    
    def set_replication(
        self,
        hdfs_path: str,
        replication: int
    ) -> bool:
        """Set replication factor for a file."""
        try:
            self.conn.set_replication(hdfs_path, replication)
            logger.info(
                f"Set replication to {replication} for: {hdfs_path}"
            )
            return True
            
        except Exception as e:
            logger.error(f"Set replication error: {e}")
            raise
    
    def get_cluster_status(self) -> Dict[str, Any]:
        """Get HDFS cluster status."""
        try:
            status = self.conn.get_capacity_information()
            return {
                'capacity': status['capacity'],
                'used': status['used'],
                'remaining': status['remaining'],
                'used_percent': (
                    status['used'] / status['capacity'] * 100
                ),
                'live_datanodes': len(
                    status.get('live_datanodes', [])
                ),
                'dead_datanodes': len(
                    status.get('dead_datanodes', [])
                )
            }
            
        except Exception as e:
            logger.error(f"Cluster status error: {e}")
            raise
    
    def close(self):
        """Close HDFS connection."""
        try:
            self.conn.close()
            logger.info("HDFS connection closed")
            
        except Exception as e:
            logger.error(f"Close connection error: {e}")
            raise

def main():
    """Example usage."""
    # Initialize HDFS client
    hdfs_ops = HDFSOperations(
        host='localhost',
        port=9000
    )
    
    try:
        # Upload example
        hdfs_ops.upload_file(
            'local_file.txt',
            '/user/test/remote_file.txt',
            replication=3
        )
        
        # Read content
        content = hdfs_ops.read_file(
            '/user/test/remote_file.txt'
        )
        print(f"File content: {content}")
        
        # Get block locations
        blocks = hdfs_ops.get_block_locations(
            '/user/test/remote_file.txt'
        )
        print(f"Block locations: {json.dumps(blocks, indent=2)}")
        
        # Get cluster status
        status = hdfs_ops.get_cluster_status()
        print(f"Cluster status: {json.dumps(status, indent=2)}")
        
    finally:
        hdfs_ops.close()

if __name__ == '__main__':
    main() 