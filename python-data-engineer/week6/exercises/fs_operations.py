"""
File System Operations
--------------------

PURPOSE:
Implement common file system operations with error handling
and logging capabilities.

FEATURES:
1. File Operations
   - Read/Write/Append
   - Copy/Move/Delete
   - Permission management

2. Directory Operations
   - Create/Delete
   - List contents
   - Recursive operations

3. Path Management
   - Path validation
   - Directory traversal
   - Symlink handling
"""

import os
import shutil
import stat
import logging
from pathlib import Path
from typing import List, Dict, Any, Union, Optional
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class FileSystemOps:
    """File system operations handler."""
    
    def __init__(self, base_path: str = None):
        """Initialize with optional base path."""
        self.base_path = Path(base_path) if base_path else Path.cwd()
        if not self.base_path.exists():
            self.base_path.mkdir(parents=True)
        logger.info(f"Initialized with base path: {self.base_path}")
    
    def safe_path(self, path: Union[str, Path]) -> Path:
        """Ensure path is safe and absolute."""
        path = Path(path)
        if not path.is_absolute():
            path = self.base_path / path
        return path.resolve()
    
    def read_file(
        self,
        path: Union[str, Path],
        mode: str = 'r',
        encoding: str = 'utf-8'
    ) -> str:
        """Read file contents safely."""
        try:
            path = self.safe_path(path)
            with open(path, mode=mode, encoding=encoding) as f:
                content = f.read()
            logger.info(f"Read file: {path}")
            return content
            
        except Exception as e:
            logger.error(f"Error reading file {path}: {e}")
            raise
    
    def write_file(
        self,
        path: Union[str, Path],
        content: str,
        mode: str = 'w',
        encoding: str = 'utf-8'
    ) -> bool:
        """Write content to file safely."""
        try:
            path = self.safe_path(path)
            with open(path, mode=mode, encoding=encoding) as f:
                f.write(content)
            logger.info(f"Wrote to file: {path}")
            return True
            
        except Exception as e:
            logger.error(f"Error writing to file {path}: {e}")
            raise
    
    def copy_file(
        self,
        src: Union[str, Path],
        dst: Union[str, Path],
        preserve_metadata: bool = True
    ) -> bool:
        """Copy file with metadata preservation option."""
        try:
            src = self.safe_path(src)
            dst = self.safe_path(dst)
            
            if preserve_metadata:
                shutil.copy2(src, dst)
            else:
                shutil.copy(src, dst)
                
            logger.info(f"Copied {src} to {dst}")
            return True
            
        except Exception as e:
            logger.error(f"Error copying {src} to {dst}: {e}")
            raise
    
    def move_file(
        self,
        src: Union[str, Path],
        dst: Union[str, Path]
    ) -> bool:
        """Move/rename file safely."""
        try:
            src = self.safe_path(src)
            dst = self.safe_path(dst)
            
            shutil.move(src, dst)
            logger.info(f"Moved {src} to {dst}")
            return True
            
        except Exception as e:
            logger.error(f"Error moving {src} to {dst}: {e}")
            raise
    
    def delete_file(
        self,
        path: Union[str, Path],
        secure: bool = False
    ) -> bool:
        """Delete file with secure option."""
        try:
            path = self.safe_path(path)
            
            if secure:
                # Overwrite with zeros before deletion
                with open(path, 'wb') as f:
                    f.write(b'\x00' * os.path.getsize(path))
                    
            os.remove(path)
            logger.info(f"Deleted file: {path}")
            return True
            
        except Exception as e:
            logger.error(f"Error deleting file {path}: {e}")
            raise
    
    def create_directory(
        self,
        path: Union[str, Path],
        mode: int = 0o755,
        parents: bool = True
    ) -> bool:
        """Create directory with specified permissions."""
        try:
            path = self.safe_path(path)
            path.mkdir(mode=mode, parents=parents, exist_ok=True)
            logger.info(f"Created directory: {path}")
            return True
            
        except Exception as e:
            logger.error(f"Error creating directory {path}: {e}")
            raise
    
    def list_directory(
        self,
        path: Union[str, Path],
        pattern: str = "*",
        recursive: bool = False
    ) -> List[Dict[str, Any]]:
        """List directory contents with metadata."""
        try:
            path = self.safe_path(path)
            
            if recursive:
                glob_pattern = f"**/{pattern}"
            else:
                glob_pattern = pattern
            
            contents = []
            for item in path.glob(glob_pattern):
                stat_info = item.stat()
                contents.append({
                    'name': item.name,
                    'path': str(item),
                    'type': 'file' if item.is_file() else 'directory',
                    'size': stat_info.st_size,
                    'modified': datetime.fromtimestamp(
                        stat_info.st_mtime
                    ),
                    'permissions': stat.filemode(stat_info.st_mode)
                })
            
            logger.info(
                f"Listed directory {path}: {len(contents)} items"
            )
            return contents
            
        except Exception as e:
            logger.error(f"Error listing directory {path}: {e}")
            raise
    
    def set_permissions(
        self,
        path: Union[str, Path],
        mode: int,
        recursive: bool = False
    ) -> bool:
        """Set file/directory permissions."""
        try:
            path = self.safe_path(path)
            
            if recursive and path.is_dir():
                for item in path.rglob("*"):
                    os.chmod(item, mode)
            
            os.chmod(path, mode)
            logger.info(
                f"Set permissions on {path}: {stat.filemode(mode)}"
            )
            return True
            
        except Exception as e:
            logger.error(
                f"Error setting permissions on {path}: {e}"
            )
            raise
    
    def get_metadata(
        self,
        path: Union[str, Path]
    ) -> Dict[str, Any]:
        """Get detailed file/directory metadata."""
        try:
            path = self.safe_path(path)
            stat_info = path.stat()
            
            return {
                'name': path.name,
                'type': 'file' if path.is_file() else 'directory',
                'size': stat_info.st_size,
                'created': datetime.fromtimestamp(
                    stat_info.st_ctime
                ),
                'modified': datetime.fromtimestamp(
                    stat_info.st_mtime
                ),
                'accessed': datetime.fromtimestamp(
                    stat_info.st_atime
                ),
                'permissions': stat.filemode(stat_info.st_mode),
                'owner': stat_info.st_uid,
                'group': stat_info.st_gid
            }
            
        except Exception as e:
            logger.error(f"Error getting metadata for {path}: {e}")
            raise

def main():
    """Example usage."""
    fs = FileSystemOps("./test_files")
    
    try:
        # Create test directory
        fs.create_directory("test_dir")
        
        # Write test file
        fs.write_file(
            "test_dir/test.txt",
            "Hello, World!"
        )
        
        # Read file
        content = fs.read_file("test_dir/test.txt")
        print(f"File content: {content}")
        
        # List directory
        contents = fs.list_directory("test_dir")
        print(f"Directory contents: {contents}")
        
        # Get file metadata
        metadata = fs.get_metadata("test_dir/test.txt")
        print(f"File metadata: {metadata}")
        
        # Copy file
        fs.copy_file(
            "test_dir/test.txt",
            "test_dir/test_copy.txt"
        )
        
        # Set permissions
        fs.set_permissions(
            "test_dir/test_copy.txt",
            0o644
        )
        
        # Clean up
        fs.delete_file("test_dir/test.txt")
        fs.delete_file("test_dir/test_copy.txt")
        os.rmdir(fs.safe_path("test_dir"))
        
    except Exception as e:
        logger.error(f"Error in main: {e}")
        raise

if __name__ == "__main__":
    main() 