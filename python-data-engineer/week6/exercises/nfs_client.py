"""
NFS Client Implementation
-----------------------

PURPOSE:
Implement NFS client operations for remote file system access.

FEATURES:
1. Mount Operations
   - Mount/Unmount
   - Auto-reconnect
   - Mount point management

2. File Operations
   - Remote file access
   - Data transfer
   - Cache management

3. Performance
   - Buffer sizing
   - Connection pooling
   - Retry mechanisms
"""

import os
import subprocess
import logging
import socket
from pathlib import Path
from typing import Dict, List, Any, Optional, Union
import time
from functools import wraps

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def retry_on_failure(max_attempts: int = 3, delay: int = 1):
    """Decorator for retrying operations."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_error = None
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_error = e
                    if attempt < max_attempts - 1:
                        time.sleep(delay)
                        logger.warning(
                            f"Retry attempt {attempt + 1} for {func.__name__}"
                        )
            raise last_error
        return wrapper
    return decorator

class NFSClient:
    """NFS client for remote file system operations."""
    
    def __init__(
        self,
        server: str,
        remote_path: str,
        mount_point: str,
        options: Dict[str, str] = None
    ):
        """Initialize NFS client."""
        self.server = server
        self.remote_path = remote_path
        self.mount_point = Path(mount_point).resolve()
        self.options = options or {
            'rw': None,          # Read-write mode
            'sync': None,        # Synchronous mode
            'vers': '4',         # NFS version
            'rsize': '1048576',  # Read buffer size
            'wsize': '1048576',  # Write buffer size
            'timeo': '600',      # Timeout (60 seconds)
            'retrans': '2',      # Number of retransmissions
            'hard': None         # Hard mount
        }
        self._mounted = False
        logger.info(f"Initialized NFS client for {server}:{remote_path}")
    
    @retry_on_failure(max_attempts=3, delay=2)
    def mount(self) -> bool:
        """Mount the remote NFS share."""
        try:
            # Create mount point if it doesn't exist
            self.mount_point.mkdir(parents=True, exist_ok=True)
            
            # Build mount options string
            options_str = ','.join(
                f"{k}={v}" if v else k
                for k, v in self.options.items()
            )
            
            # Check if already mounted
            if self.is_mounted():
                logger.info(f"Already mounted at {self.mount_point}")
                return True
            
            # Construct mount command
            cmd = [
                'mount',
                '-t', 'nfs',
                '-o', options_str,
                f"{self.server}:{self.remote_path}",
                str(self.mount_point)
            ]
            
            # Execute mount command
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True
            )
            
            self._mounted = True
            logger.info(
                f"Mounted {self.server}:{self.remote_path} "
                f"at {self.mount_point}"
            )
            return True
            
        except subprocess.CalledProcessError as e:
            logger.error(
                f"Mount error: {e.stderr.strip()}"
            )
            raise
        except Exception as e:
            logger.error(f"Mount error: {e}")
            raise
    
    @retry_on_failure(max_attempts=3, delay=1)
    def unmount(self, force: bool = False) -> bool:
        """Unmount the NFS share."""
        try:
            if not self.is_mounted():
                logger.info("Not currently mounted")
                return True
            
            cmd = ['umount']
            if force:
                cmd.append('-f')
            cmd.append(str(self.mount_point))
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True
            )
            
            self._mounted = False
            logger.info(f"Unmounted {self.mount_point}")
            return True
            
        except subprocess.CalledProcessError as e:
            logger.error(
                f"Unmount error: {e.stderr.strip()}"
            )
            raise
        except Exception as e:
            logger.error(f"Unmount error: {e}")
            raise
    
    def is_mounted(self) -> bool:
        """Check if the NFS share is mounted."""
        try:
            with open('/proc/mounts', 'r') as f:
                mounts = f.read()
            return str(self.mount_point) in mounts
            
        except Exception as e:
            logger.error(f"Mount check error: {e}")
            return False
    
    def get_mount_stats(self) -> Dict[str, Any]:
        """Get NFS mount statistics."""
        try:
            if not self.is_mounted():
                raise RuntimeError("NFS share not mounted")
            
            cmd = ['nfsstat', '-m', str(self.mount_point)]
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True
            )
            
            # Parse nfsstat output
            stats = {}
            for line in result.stdout.split('\n'):
                if ':' in line:
                    key, value = line.split(':', 1)
                    stats[key.strip()] = value.strip()
            
            return stats
            
        except subprocess.CalledProcessError as e:
            logger.error(
                f"Stats error: {e.stderr.strip()}"
            )
            raise
        except Exception as e:
            logger.error(f"Stats error: {e}")
            raise
    
    def check_connectivity(self) -> bool:
        """Check NFS server connectivity."""
        try:
            # Try to connect to NFS port (2049)
            with socket.create_connection(
                (self.server, 2049),
                timeout=5
            ):
                return True
        except Exception:
            return False
    
    def remount_if_needed(self) -> bool:
        """Remount if connection is lost."""
        try:
            if not self.is_mounted() or not self.check_connectivity():
                logger.warning("Lost connection, attempting remount")
                self.unmount(force=True)
                return self.mount()
            return True
            
        except Exception as e:
            logger.error(f"Remount error: {e}")
            return False
    
    def get_space_info(self) -> Dict[str, int]:
        """Get space information for the mount."""
        try:
            if not self.is_mounted():
                raise RuntimeError("NFS share not mounted")
            
            stat = os.statvfs(self.mount_point)
            
            return {
                'total': stat.f_blocks * stat.f_frsize,
                'free': stat.f_bfree * stat.f_frsize,
                'available': stat.f_bavail * stat.f_frsize
            }
            
        except Exception as e:
            logger.error(f"Space info error: {e}")
            raise
    
    def __enter__(self):
        """Context manager entry."""
        self.mount()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.unmount()

def main():
    """Example usage."""
    # NFS server configuration
    config = {
        'server': 'nfs.example.com',
        'remote_path': '/shared',
        'mount_point': '/mnt/nfs',
        'options': {
            'rw': None,
            'sync': None,
            'vers': '4',
            'rsize': '1048576',
            'wsize': '1048576'
        }
    }
    
    try:
        # Use context manager for automatic mounting/unmounting
        with NFSClient(**config) as nfs:
            # Check space information
            space_info = nfs.get_space_info()
            print(f"Space info: {space_info}")
            
            # Get mount statistics
            stats = nfs.get_mount_stats()
            print(f"Mount stats: {stats}")
            
    except Exception as e:
        logger.error(f"Error in main: {e}")
        raise

if __name__ == "__main__":
    main() 