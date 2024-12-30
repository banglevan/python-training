"""
Storage Optimization
-----------------

PURPOSE:
Implement storage optimization techniques with:
1. Compression
   - Multiple algorithms
   - Adaptive selection
   - Performance metrics

2. Deduplication
   - Block-level
   - File-level
   - Content-aware

3. Analysis
   - Space savings
   - Performance impact
   - Resource usage
"""

import logging
import zlib
import lz4.frame
import zstandard
import xxhash
import os
from pathlib import Path
from typing import Dict, List, Any, Optional, Union, BinaryIO, Tuple
import json
from datetime import datetime
import io
import hashlib
from concurrent.futures import ThreadPoolExecutor
import time
import psutil

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class StorageOptimizer:
    """Storage optimization handler."""
    
    def __init__(
        self,
        work_dir: Union[str, Path],
        block_size: int = 1024 * 1024,  # 1MB
        threads: int = 4
    ):
        """Initialize optimizer."""
        self.work_dir = Path(work_dir)
        self.work_dir.mkdir(parents=True, exist_ok=True)
        self.block_size = block_size
        self.threads = threads
        
        # Initialize deduplication store
        self.dedup_store = self.work_dir / 'dedup_store'
        self.dedup_store.mkdir(exist_ok=True)
        
        # Load existing dedup index
        self.dedup_index = self._load_dedup_index()
        
        logger.info(
            f"Initialized StorageOptimizer in {self.work_dir}"
        )
    
    def compress_file(
        self,
        file_path: Union[str, Path],
        algorithm: str = 'auto',
        level: Optional[int] = None
    ) -> Dict[str, Any]:
        """Compress file using selected algorithm."""
        try:
            file_path = Path(file_path)
            if not file_path.exists():
                raise FileNotFoundError(
                    f"File not found: {file_path}"
                )
            
            # Read original file
            with open(file_path, 'rb') as f:
                data = f.read()
            
            original_size = len(data)
            
            # Select best algorithm if auto
            if algorithm == 'auto':
                algorithm = self._select_best_algorithm(data)
            
            # Compress data
            start_time = time.time()
            compressed_data = self._compress_data(
                data,
                algorithm,
                level
            )
            compression_time = time.time() - start_time
            
            # Calculate metrics
            compressed_size = len(compressed_data)
            ratio = original_size / compressed_size
            
            # Save compressed file
            compressed_path = file_path.with_suffix(
                f'.{algorithm}'
            )
            with open(compressed_path, 'wb') as f:
                f.write(compressed_data)
            
            result = {
                'algorithm': algorithm,
                'original_size': original_size,
                'compressed_size': compressed_size,
                'ratio': ratio,
                'compression_time': compression_time,
                'compressed_path': str(compressed_path)
            }
            
            logger.info(
                f"Compressed {file_path} with {algorithm}, "
                f"ratio: {ratio:.2f}"
            )
            return result
            
        except Exception as e:
            logger.error(f"Compression error: {e}")
            raise
    
    def decompress_file(
        self,
        file_path: Union[str, Path],
        output_path: Optional[Union[str, Path]] = None
    ) -> Dict[str, Any]:
        """Decompress file."""
        try:
            file_path = Path(file_path)
            if not file_path.exists():
                raise FileNotFoundError(
                    f"File not found: {file_path}"
                )
            
            # Determine algorithm from extension
            algorithm = file_path.suffix[1:]  # Remove dot
            
            # Read compressed file
            with open(file_path, 'rb') as f:
                compressed_data = f.read()
            
            # Decompress data
            start_time = time.time()
            decompressed_data = self._decompress_data(
                compressed_data,
                algorithm
            )
            decompression_time = time.time() - start_time
            
            # Save decompressed file
            output_path = output_path or file_path.with_suffix('')
            with open(output_path, 'wb') as f:
                f.write(decompressed_data)
            
            result = {
                'algorithm': algorithm,
                'compressed_size': len(compressed_data),
                'decompressed_size': len(decompressed_data),
                'decompression_time': decompression_time,
                'output_path': str(output_path)
            }
            
            logger.info(
                f"Decompressed {file_path} to {output_path}"
            )
            return result
            
        except Exception as e:
            logger.error(f"Decompression error: {e}")
            raise
    
    def deduplicate_file(
        self,
        file_path: Union[str, Path],
        block_size: Optional[int] = None
    ) -> Dict[str, Any]:
        """Perform file-level deduplication."""
        try:
            file_path = Path(file_path)
            if not file_path.exists():
                raise FileNotFoundError(
                    f"File not found: {file_path}"
                )
            
            block_size = block_size or self.block_size
            total_blocks = 0
            unique_blocks = 0
            saved_space = 0
            
            # Process file in blocks
            with open(file_path, 'rb') as f:
                while True:
                    block = f.read(block_size)
                    if not block:
                        break
                    
                    total_blocks += 1
                    
                    # Generate block hash
                    block_hash = xxhash.xxh64(block).hexdigest()
                    
                    # Check if block exists
                    if block_hash not in self.dedup_index:
                        # Store new block
                        block_path = self.dedup_store / block_hash
                        with open(block_path, 'wb') as bf:
                            bf.write(block)
                        
                        self.dedup_index[block_hash] = {
                            'size': len(block),
                            'references': 0
                        }
                        unique_blocks += 1
                    
                    # Update reference count
                    self.dedup_index[block_hash]['references'] += 1
                    saved_space += len(block)
            
            # Save updated index
            self._save_dedup_index()
            
            result = {
                'total_blocks': total_blocks,
                'unique_blocks': unique_blocks,
                'duplicate_blocks': total_blocks - unique_blocks,
                'saved_space': saved_space,
                'dedup_ratio': total_blocks / unique_blocks
                if unique_blocks > 0 else 0
            }
            
            logger.info(
                f"Deduplicated {file_path}, "
                f"ratio: {result['dedup_ratio']:.2f}"
            )
            return result
            
        except Exception as e:
            logger.error(f"Deduplication error: {e}")
            raise
    
    def analyze_storage(
        self,
        path: Union[str, Path]
    ) -> Dict[str, Any]:
        """Analyze storage usage and optimization potential."""
        try:
            path = Path(path)
            if not path.exists():
                raise FileNotFoundError(
                    f"Path not found: {path}"
                )
            
            start_time = time.time()
            
            # Collect file statistics
            total_size = 0
            file_count = 0
            extension_stats = {}
            duplicate_files = []
            
            # Process files
            for file_path in path.rglob('*'):
                if file_path.is_file():
                    file_count += 1
                    size = file_path.stat().st_size
                    total_size += size
                    
                    # Track extension statistics
                    ext = file_path.suffix.lower()
                    if ext not in extension_stats:
                        extension_stats[ext] = {
                            'count': 0,
                            'total_size': 0
                        }
                    extension_stats[ext]['count'] += 1
                    extension_stats[ext]['total_size'] += size
                    
                    # Check for potential duplicates
                    file_hash = self._calculate_file_hash(file_path)
                    duplicate_files.append({
                        'path': str(file_path),
                        'size': size,
                        'hash': file_hash
                    })
            
            # Find actual duplicates
            duplicates = self._find_duplicates(duplicate_files)
            
            # Calculate system metrics
            disk_usage = psutil.disk_usage(str(path))
            
            analysis_time = time.time() - start_time
            
            result = {
                'total_size': total_size,
                'file_count': file_count,
                'extension_stats': extension_stats,
                'duplicates': duplicates,
                'disk_usage': {
                    'total': disk_usage.total,
                    'used': disk_usage.used,
                    'free': disk_usage.free,
                    'percent': disk_usage.percent
                },
                'analysis_time': analysis_time
            }
            
            logger.info(
                f"Analyzed storage at {path}, "
                f"found {len(duplicates)} duplicate sets"
            )
            return result
            
        except Exception as e:
            logger.error(f"Storage analysis error: {e}")
            raise
    
    def _compress_data(
        self,
        data: bytes,
        algorithm: str,
        level: Optional[int] = None
    ) -> bytes:
        """Compress data using specified algorithm."""
        if algorithm == 'zlib':
            level = level or zlib.Z_BEST_COMPRESSION
            return zlib.compress(data, level)
        elif algorithm == 'lz4':
            return lz4.frame.compress(
                data,
                compression_level=level or 0
            )
        elif algorithm == 'zstd':
            level = level or 3
            cctx = zstandard.ZstdCompressor(level=level)
            return cctx.compress(data)
        else:
            raise ValueError(f"Unknown algorithm: {algorithm}")
    
    def _decompress_data(
        self,
        data: bytes,
        algorithm: str
    ) -> bytes:
        """Decompress data using specified algorithm."""
        if algorithm == 'zlib':
            return zlib.decompress(data)
        elif algorithm == 'lz4':
            return lz4.frame.decompress(data)
        elif algorithm == 'zstd':
            dctx = zstandard.ZstdDecompressor()
            return dctx.decompress(data)
        else:
            raise ValueError(f"Unknown algorithm: {algorithm}")
    
    def _select_best_algorithm(
        self,
        data: bytes,
        sample_size: int = 1024 * 1024  # 1MB
    ) -> str:
        """Select best compression algorithm for data."""
        # Use a sample of data for large files
        if len(data) > sample_size:
            data = data[:sample_size]
        
        results = {}
        for algo in ['zlib', 'lz4', 'zstd']:
            try:
                start_time = time.time()
                compressed = self._compress_data(data, algo)
                compression_time = time.time() - start_time
                
                results[algo] = {
                    'ratio': len(data) / len(compressed),
                    'speed': len(data) / compression_time
                }
            except Exception as e:
                logger.warning(
                    f"Algorithm {algo} failed: {e}"
                )
        
        # Select algorithm with best balance of ratio and speed
        best_algo = max(
            results.keys(),
            key=lambda x: results[x]['ratio'] * results[x]['speed']
        )
        
        return best_algo
    
    def _load_dedup_index(self) -> Dict[str, Dict[str, Any]]:
        """Load deduplication index."""
        index_path = self.work_dir / 'dedup_index.json'
        if index_path.exists():
            with open(index_path, 'r') as f:
                return json.load(f)
        return {}
    
    def _save_dedup_index(self):
        """Save deduplication index."""
        index_path = self.work_dir / 'dedup_index.json'
        with open(index_path, 'w') as f:
            json.dump(self.dedup_index, f)
    
    def _calculate_file_hash(
        self,
        file_path: Path,
        chunk_size: int = 8192
    ) -> str:
        """Calculate file hash."""
        hasher = hashlib.sha256()
        with open(file_path, 'rb') as f:
            while chunk := f.read(chunk_size):
                hasher.update(chunk)
        return hasher.hexdigest()
    
    def _find_duplicates(
        self,
        files: List[Dict[str, Any]]
    ) -> List[List[Dict[str, Any]]]:
        """Find duplicate files."""
        # Group files by hash
        hash_groups = {}
        for file_info in files:
            file_hash = file_info['hash']
            if file_hash not in hash_groups:
                hash_groups[file_hash] = []
            hash_groups[file_hash].append(file_info)
        
        # Return only groups with duplicates
        return [
            group for group in hash_groups.values()
            if len(group) > 1
        ]

def main():
    """Example usage."""
    optimizer = StorageOptimizer('storage_opt_work')
    
    try:
        # Compress file
        compress_result = optimizer.compress_file(
            'test_data.txt',
            algorithm='auto'
        )
        print(f"Compression result: {json.dumps(compress_result, indent=2)}")
        
        # Deduplicate file
        dedup_result = optimizer.deduplicate_file(
            'test_data.txt'
        )
        print(f"Deduplication result: {json.dumps(dedup_result, indent=2)}")
        
        # Analyze storage
        analysis = optimizer.analyze_storage('data_directory')
        print(f"Storage analysis: {json.dumps(analysis, indent=2)}")
        
    except Exception as e:
        logger.error(f"Error in main: {e}")
        raise

if __name__ == '__main__':
    main() 