"""
Format handlers for Data Lake Platform.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
import logging
from PIL import Image
import io
import json
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.types import *

logger = logging.getLogger(__name__)

class FormatHandler(ABC):
    """Abstract base class for format handlers."""
    
    @abstractmethod
    def read(self, data: Any) -> Any:
        """Read data in specific format."""
        pass
    
    @abstractmethod
    def write(self, data: Any, **options: Dict[str, Any]) -> Any:
        """Write data in specific format."""
        pass

class ImageHandler(FormatHandler):
    """Handle image formats."""
    
    def read(self, data: bytes) -> Dict[str, Any]:
        """
        Read image data.
        
        Args:
            data: Raw image bytes
        
        Returns:
            Image metadata and content
        """
        try:
            # Open image from bytes
            img = Image.open(io.BytesIO(data))
            
            # Extract metadata
            metadata = {
                'format': img.format,
                'mode': img.mode,
                'width': img.width,
                'height': img.height,
                'size': len(data)
            }
            
            return {
                'content': data,
                'metadata': metadata
            }
            
        except Exception as e:
            logger.error(f"Failed to read image: {e}")
            raise
    
    def write(
        self,
        data: Dict[str, Any],
        format: str = "JPEG",
        **options: Dict[str, Any]
    ) -> bytes:
        """
        Write image data.
        
        Args:
            data: Image data
            format: Output format
            options: Additional options
        
        Returns:
            Image bytes
        """
        try:
            img = Image.open(io.BytesIO(data['content']))
            
            # Convert if needed
            if format and img.format != format:
                output = io.BytesIO()
                img.save(output, format=format, **options)
                return output.getvalue()
            
            return data['content']
            
        except Exception as e:
            logger.error(f"Failed to write image: {e}")
            raise

class MetadataHandler(FormatHandler):
    """Handle metadata formats."""
    
    def read(
        self,
        data: str,
        format: str = "json"
    ) -> Dict[str, Any]:
        """
        Read metadata.
        
        Args:
            data: Raw metadata
            format: Input format
        
        Returns:
            Parsed metadata
        """
        try:
            if format.lower() == "json":
                return json.loads(data)
            elif format.lower() == "yaml":
                return yaml.safe_load(data)
            else:
                raise ValueError(f"Unsupported format: {format}")
            
        except Exception as e:
            logger.error(f"Failed to read metadata: {e}")
            raise
    
    def write(
        self,
        data: Dict[str, Any],
        format: str = "json",
        **options: Dict[str, Any]
    ) -> str:
        """
        Write metadata.
        
        Args:
            data: Metadata to write
            format: Output format
            options: Additional options
        
        Returns:
            Formatted metadata
        """
        try:
            if format.lower() == "json":
                return json.dumps(data, **options)
            elif format.lower() == "yaml":
                return yaml.dump(data, **options)
            else:
                raise ValueError(f"Unsupported format: {format}")
            
        except Exception as e:
            logger.error(f"Failed to write metadata: {e}")
            raise

class DeltaHandler(FormatHandler):
    """Handle Delta format."""
    
    def __init__(self, spark: SparkSession):
        """Initialize Delta handler."""
        self.spark = spark
    
    def read(
        self,
        path: str,
        **options: Dict[str, Any]
    ) -> DataFrame:
        """
        Read Delta table.
        
        Args:
            path: Table path
            options: Read options
        
        Returns:
            DataFrame
        """
        try:
            return self.spark.read.format("delta") \
                .options(**options) \
                .load(path)
                
        except Exception as e:
            logger.error(f"Failed to read Delta table: {e}")
            raise
    
    def write(
        self,
        data: DataFrame,
        path: str,
        mode: str = "append",
        **options: Dict[str, Any]
    ) -> None:
        """
        Write Delta table.
        
        Args:
            data: DataFrame to write
            path: Table path
            mode: Write mode
            options: Write options
        """
        try:
            data.write.format("delta") \
                .options(**options) \
                .mode(mode) \
                .save(path)
                
        except Exception as e:
            logger.error(f"Failed to write Delta table: {e}")
            raise 