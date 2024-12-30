"""
Metadata extraction components.
"""

from typing import Dict, Any, Optional, List
import logging
import json
from datetime import datetime
from PIL import Image
import io
import exifread
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

logger = logging.getLogger(__name__)

class MetadataProcessor:
    """Metadata extraction operations."""
    
    def __init__(self, spark: SparkSession):
        """Initialize processor."""
        self.spark = spark
    
    def extract_exif(
        self,
        data: DataFrame,
        content_col: str = "content",
        metadata_col: str = "exif_metadata"
    ) -> DataFrame:
        """
        Extract EXIF metadata.
        
        Args:
            data: Input DataFrame
            content_col: Column containing image data
            metadata_col: Output metadata column
        
        Returns:
            DataFrame with EXIF metadata
        """
        def exif_udf(content: bytes) -> Optional[Dict[str, Any]]:
            try:
                # Read EXIF data
                tags = exifread.process_file(io.BytesIO(content))
                
                # Convert to dictionary
                metadata = {}
                for tag, value in tags.items():
                    # Clean tag name
                    key = tag.split()[-1]
                    # Convert value to string
                    metadata[key] = str(value)
                
                return metadata
                
            except Exception as e:
                logger.error(f"Failed to extract EXIF: {e}")
                return None
        
        # Register UDF
        extract = udf(exif_udf, MapType(StringType(), StringType()))
        
        # Apply transformation
        return data.withColumn(
            metadata_col,
            extract(col(content_col))
        )
    
    def extract_image_info(
        self,
        data: DataFrame,
        content_col: str = "content",
        info_col: str = "image_info"
    ) -> DataFrame:
        """
        Extract basic image information.
        
        Args:
            data: Input DataFrame
            content_col: Column containing image data
            info_col: Output info column
        
        Returns:
            DataFrame with image info
        """
        def info_udf(content: bytes) -> Optional[Dict[str, Any]]:
            try:
                # Open image
                img = Image.open(io.BytesIO(content))
                
                return {
                    'format': img.format,
                    'mode': img.mode,
                    'width': img.width,
                    'height': img.height,
                    'size': len(content),
                    'aspect_ratio': round(img.width / img.height, 2)
                }
                
            except Exception as e:
                logger.error(f"Failed to extract image info: {e}")
                return None
        
        # Register UDF
        extract = udf(info_udf, MapType(StringType(), StringType()))
        
        # Apply transformation
        return data.withColumn(
            info_col,
            extract(col(content_col))
        )
    
    def generate_tags(
        self,
        data: DataFrame,
        metadata_col: str = "metadata",
        tags_col: str = "tags"
    ) -> DataFrame:
        """
        Generate tags from metadata.
        
        Args:
            data: Input DataFrame
            metadata_col: Column containing metadata
            tags_col: Output tags column
        
        Returns:
            DataFrame with generated tags
        """
        def tags_udf(metadata: Dict[str, Any]) -> List[str]:
            tags = set()
            
            try:
                if not metadata:
                    return []
                
                # Add format tag
                if 'format' in metadata:
                    tags.add(f"format:{metadata['format'].lower()}")
                
                # Add size category
                if 'size' in metadata:
                    size = int(metadata['size'])
                    if size < 1024 * 1024:  # < 1MB
                        tags.add("size:small")
                    elif size < 5 * 1024 * 1024:  # < 5MB
                        tags.add("size:medium")
                    else:
                        tags.add("size:large")
                
                # Add resolution category
                if 'width' in metadata and 'height' in metadata:
                    resolution = int(metadata['width']) * int(metadata['height'])
                    if resolution < 1920 * 1080:
                        tags.add("resolution:low")
                    elif resolution < 3840 * 2160:
                        tags.add("resolution:medium")
                    else:
                        tags.add("resolution:high")
                
                return list(tags)
                
            except Exception as e:
                logger.error(f"Failed to generate tags: {e}")
                return []
        
        # Register UDF
        generate = udf(tags_udf, ArrayType(StringType()))
        
        # Apply transformation
        return data.withColumn(
            tags_col,
            generate(col(metadata_col))
        ) 