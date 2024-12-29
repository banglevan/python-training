"""
Ceph Storage Operations
---------------------

PURPOSE:
Implement Ceph storage operations across different interfaces:
1. RADOS (Object Storage)
2. RBD (Block Storage)
3. CephFS (File System)

FEATURES:
1. Object Operations
   - Create/Read/Write/Delete
   - Pool management
   - Object versioning

2. Block Operations
   - Volume management
   - Snapshots
   - Cloning

3. File Operations
   - Mount management
   - Directory operations
   - File handling
"""

import rados
import rbd
import logging
import json
from typing import Dict, List, Any, Optional, Union, BinaryIO
from pathlib import Path
import subprocess
from datetime import datetime
import io

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CephStorage:
    """Ceph storage operations handler."""
    
    def __init__(
        self,
        conf_file: str = '/etc/ceph/ceph.conf',
        pool_name: str = 'rbd',
        user_name: str = 'admin',
        cluster_name: str = 'ceph'
    ):
        """Initialize Ceph connections."""
        try:
            # Initialize RADOS connection
            self.cluster = rados.Rados(
                conffile=conf_file,
                name=f'client.{user_name}',
                clustername=cluster_name
            )
            self.cluster.connect()
            
            # Initialize pool
            self.pool_name = pool_name
            self.ioctx = self.cluster.open_ioctx(pool_name)
            
            # Initialize RBD
            self.rbd = rbd.RBD()
            
            logger.info(
                f"Connected to Ceph cluster: {cluster_name}"
            )
            
        except Exception as e:
            logger.error(f"Ceph initialization error: {e}")
            raise
    
    def list_pools(self) -> List[str]:
        """List available Ceph pools."""
        try:
            return self.cluster.list_pools()
            
        except Exception as e:
            logger.error(f"List pools error: {e}")
            raise
    
    def create_pool(
        self,
        pool_name: str,
        pg_num: int = 32,
        pgp_num: int = 32
    ) -> bool:
        """Create a new Ceph pool."""
        try:
            self.cluster.create_pool(
                pool_name,
                pg_num,
                pgp_num
            )
            logger.info(f"Created pool: {pool_name}")
            return True
            
        except Exception as e:
            logger.error(f"Create pool error: {e}")
            raise
    
    def delete_pool(self, pool_name: str) -> bool:
        """Delete a Ceph pool."""
        try:
            self.cluster.delete_pool(pool_name)
            logger.info(f"Deleted pool: {pool_name}")
            return True
            
        except Exception as e:
            logger.error(f"Delete pool error: {e}")
            raise
    
    # Object Storage (RADOS) Operations
    def write_object(
        self,
        obj_name: str,
        data: Union[str, bytes],
        encoding: str = 'utf-8'
    ) -> bool:
        """Write object to Ceph storage."""
        try:
            if isinstance(data, str):
                data = data.encode(encoding)
            
            self.ioctx.write_full(obj_name, data)
            logger.info(f"Wrote object: {obj_name}")
            return True
            
        except Exception as e:
            logger.error(f"Write object error: {e}")
            raise
    
    def read_object(
        self,
        obj_name: str,
        encoding: str = 'utf-8'
    ) -> Union[str, bytes]:
        """Read object from Ceph storage."""
        try:
            data = self.ioctx.read(obj_name)
            return data.decode(encoding)
            
        except Exception as e:
            logger.error(f"Read object error: {e}")
            raise
    
    def delete_object(self, obj_name: str) -> bool:
        """Delete object from Ceph storage."""
        try:
            self.ioctx.remove_object(obj_name)
            logger.info(f"Deleted object: {obj_name}")
            return True
            
        except Exception as e:
            logger.error(f"Delete object error: {e}")
            raise
    
    def list_objects(self) -> List[str]:
        """List objects in current pool."""
        try:
            return [obj for obj in self.ioctx.list_objects()]
            
        except Exception as e:
            logger.error(f"List objects error: {e}")
            raise
    
    # Block Storage (RBD) Operations
    def create_image(
        self,
        name: str,
        size: int,  # Size in bytes
        features: List[int] = None,
        old_format: bool = False
    ) -> bool:
        """Create a new RBD image."""
        try:
            self.rbd.create(
                self.ioctx,
                name,
                size,
                features=features,
                old_format=old_format
            )
            logger.info(
                f"Created image: {name} ({size} bytes)"
            )
            return True
            
        except Exception as e:
            logger.error(f"Create image error: {e}")
            raise
    
    def remove_image(self, name: str) -> bool:
        """Remove an RBD image."""
        try:
            self.rbd.remove(self.ioctx, name)
            logger.info(f"Removed image: {name}")
            return True
            
        except Exception as e:
            logger.error(f"Remove image error: {e}")
            raise
    
    def create_snapshot(
        self,
        image_name: str,
        snapshot_name: str
    ) -> bool:
        """Create image snapshot."""
        try:
            with rbd.Image(self.ioctx, image_name) as image:
                image.create_snap(snapshot_name)
            
            logger.info(
                f"Created snapshot: {image_name}@{snapshot_name}"
            )
            return True
            
        except Exception as e:
            logger.error(f"Create snapshot error: {e}")
            raise
    
    def remove_snapshot(
        self,
        image_name: str,
        snapshot_name: str
    ) -> bool:
        """Remove image snapshot."""
        try:
            with rbd.Image(self.ioctx, image_name) as image:
                image.remove_snap(snapshot_name)
            
            logger.info(
                f"Removed snapshot: {image_name}@{snapshot_name}"
            )
            return True
            
        except Exception as e:
            logger.error(f"Remove snapshot error: {e}")
            raise
    
    def clone_image(
        self,
        parent_name: str,
        snapshot_name: str,
        clone_name: str
    ) -> bool:
        """Clone image from snapshot."""
        try:
            with rbd.Image(self.ioctx, parent_name) as parent:
                parent.protect_snap(snapshot_name)
                self.rbd.clone(
                    self.ioctx,
                    parent_name,
                    snapshot_name,
                    self.ioctx,
                    clone_name
                )
            
            logger.info(
                f"Cloned {parent_name}@{snapshot_name} to {clone_name}"
            )
            return True
            
        except Exception as e:
            logger.error(f"Clone image error: {e}")
            raise
    
    # CephFS Operations
    def mount_fs(
        self,
        mount_point: Union[str, Path],
        mon_host: str,
        user_name: str = 'admin',
        fs_name: str = 'cephfs'
    ) -> bool:
        """Mount CephFS filesystem."""
        try:
            mount_point = Path(mount_point)
            mount_point.mkdir(parents=True, exist_ok=True)
            
            cmd = [
                'mount',
                '-t', 'ceph',
                f'{mon_host}:/', str(mount_point),
                '-o',
                f'name={user_name},fs={fs_name}'
            ]
            
            subprocess.run(cmd, check=True)
            logger.info(f"Mounted CephFS at: {mount_point}")
            return True
            
        except Exception as e:
            logger.error(f"Mount error: {e}")
            raise
    
    def unmount_fs(
        self,
        mount_point: Union[str, Path]
    ) -> bool:
        """Unmount CephFS filesystem."""
        try:
            cmd = ['umount', str(mount_point)]
            subprocess.run(cmd, check=True)
            logger.info(f"Unmounted CephFS from: {mount_point}")
            return True
            
        except Exception as e:
            logger.error(f"Unmount error: {e}")
            raise
    
    def get_cluster_status(self) -> Dict[str, Any]:
        """Get Ceph cluster status."""
        try:
            cmd = ['ceph', 'status', '--format=json']
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True
            )
            
            status = json.loads(result.stdout)
            return {
                'health': status['health']['status'],
                'monmap': status['monmap'],
                'osdmap': status['osdmap'],
                'pgmap': status['pgmap'],
                'fsmap': status.get('fsmap', {}),
                'timestamp': datetime.utcnow()
            }
            
        except Exception as e:
            logger.error(f"Cluster status error: {e}")
            raise
    
    def close(self):
        """Close Ceph connections."""
        try:
            if hasattr(self, 'ioctx'):
                self.ioctx.close()
            if hasattr(self, 'cluster'):
                self.cluster.shutdown()
            logger.info("Ceph connections closed")
            
        except Exception as e:
            logger.error(f"Close connection error: {e}")
            raise

def main():
    """Example usage."""
    ceph = CephStorage(
        conf_file='/etc/ceph/ceph.conf',
        pool_name='rbd',
        user_name='admin'
    )
    
    try:
        # Object storage example
        ceph.write_object(
            'test-object',
            'Hello, Ceph!'
        )
        
        content = ceph.read_object('test-object')
        print(f"Object content: {content}")
        
        # Block storage example
        ceph.create_image(
            'test-image',
            1 * 1024 * 1024 * 1024  # 1GB
        )
        
        ceph.create_snapshot(
            'test-image',
            'snapshot1'
        )
        
        # Get cluster status
        status = ceph.get_cluster_status()
        print(f"Cluster status: {json.dumps(status, indent=2)}")
        
    finally:
        ceph.close()

if __name__ == '__main__':
    main() 