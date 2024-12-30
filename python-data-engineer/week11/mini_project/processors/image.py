"""
Image processing components.
"""

from typing import Dict, Any, Optional, List, Tuple
import logging
import io
from PIL import Image
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

logger = logging.getLogger(__name__)

class ImageProcessor:
    """Image processing operations."""
    
    def __init__(self, spark: SparkSession):
        """Initialize processor."""
        self.spark = spark
    
    def resize_images(
        self,
        data: DataFrame,
        size: Tuple[int, int],
        content_col: str = "content",
        output_col: str = "resized_content"
    ) -> DataFrame:
        """
        Resize images in DataFrame.
        
        Args:
            data: Input DataFrame
            size: Target size (width, height)
            content_col: Column containing image data
            output_col: Output column name
        
        Returns:
            DataFrame with resized images
        """
        def resize_udf(content: bytes) -> Optional[bytes]:
            try:
                # Open image
                img = Image.open(io.BytesIO(content))
                
                # Resize
                resized = img.resize(size, Image.Resampling.LANCZOS)
                
                # Convert back to bytes
                buffer = io.BytesIO()
                resized.save(buffer, format=img.format)
                return buffer.getvalue()
                
            except Exception as e:
                logger.error(f"Failed to resize image: {e}")
                return None
        
        # Register UDF
        resize = udf(resize_udf, BinaryType())
        
        # Apply transformation
        return data.withColumn(
            output_col,
            resize(col(content_col))
        )
    
    def extract_features(
        self,
        data: DataFrame,
        content_col: str = "content",
        feature_col: str = "features"
    ) -> DataFrame:
        """
        Extract image features.
        
        Args:
            data: Input DataFrame
            content_col: Column containing image data
            feature_col: Output feature column
        
        Returns:
            DataFrame with features
        """
        def extract_udf(content: bytes) -> Optional[List[float]]:
            try:
                # Open image
                img = Image.open(io.BytesIO(content))
                
                # Convert to grayscale
                gray = img.convert('L')
                
                # Resize to fixed size for feature extraction
                resized = gray.resize((64, 64))
                
                # Convert to numpy array and normalize
                array = np.array(resized).flatten() / 255.0
                
                return array.tolist()
                
            except Exception as e:
                logger.error(f"Failed to extract features: {e}")
                return None
        
        # Register UDF
        extract = udf(extract_udf, ArrayType(FloatType()))
        
        # Apply transformation
        return data.withColumn(
            feature_col,
            extract(col(content_col))
        )
    
    def apply_filters(
        self,
        data: DataFrame,
        filters: List[str],
        content_col: str = "content",
        output_col: str = "filtered_content"
    ) -> DataFrame:
        """
        Apply image filters.
        
        Args:
            data: Input DataFrame
            filters: List of filters to apply
            content_col: Column containing image data
            output_col: Output column name
        
        Returns:
            DataFrame with filtered images
        """
        def filter_udf(content: bytes) -> Optional[bytes]:
            try:
                # Open image
                img = Image.open(io.BytesIO(content))
                
                # Apply filters
                for filter_name in filters:
                    if filter_name == "BLUR":
                        img = img.filter(Image.BLUR)
                    elif filter_name == "SHARPEN":
                        img = img.filter(Image.SHARPEN)
                    elif filter_name == "EDGE_ENHANCE":
                        img = img.filter(Image.EDGE_ENHANCE)
                
                # Convert back to bytes
                buffer = io.BytesIO()
                img.save(buffer, format=img.format)
                return buffer.getvalue()
                
            except Exception as e:
                logger.error(f"Failed to apply filters: {e}")
                return None
        
        # Register UDF
        apply_filter = udf(filter_udf, BinaryType())
        
        # Apply transformation
        return data.withColumn(
            output_col,
            apply_filter(col(content_col))
        ) 