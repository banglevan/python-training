"""
Data validation utilities.
"""

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from typing import Dict, Any, List
import logging

logger = logging.getLogger(__name__)

class DataValidator:
    """Validate data quality."""
    
    def validate_dataframe(
        self,
        df: DataFrame,
        rules: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Validate DataFrame against rules.
        
        Args:
            df: DataFrame to validate
            rules: Validation rules
            
        Returns:
            Validation results
        """
        try:
            errors = []
            
            # Check schema
            if 'schema' in rules:
                schema_errors = self._validate_schema(
                    df, rules['schema']
                )
                errors.extend(schema_errors)
            
            # Check nulls
            if 'null_checks' in rules:
                null_errors = self._check_nulls(
                    df, rules['null_checks']
                )
                errors.extend(null_errors)
            
            # Check data types
            if 'type_checks' in rules:
                type_errors = self._check_types(
                    df, rules['type_checks']
                )
                errors.extend(type_errors)
            
            # Check value ranges
            if 'range_checks' in rules:
                range_errors = self._check_ranges(
                    df, rules['range_checks']
                )
                errors.extend(range_errors)
            
            return {
                'passed': len(errors) == 0,
                'errors': errors
            }
            
        except Exception as e:
            logger.error(f"Validation failed: {e}")
            raise
    
    def _validate_schema(
        self,
        df: DataFrame,
        expected_schema: StructType
    ) -> List[str]:
        """Validate DataFrame schema."""
        errors = []
        actual_fields = df.schema.fields
        expected_fields = expected_schema.fields
        
        # Check missing columns
        actual_cols = set(f.name for f in actual_fields)
        expected_cols = set(f.name for f in expected_fields)
        
        missing_cols = expected_cols - actual_cols
        if missing_cols:
            errors.append(
                f"Missing columns: {missing_cols}"
            )
        
        # Check data types
        for expected_field in expected_fields:
            if expected_field.name in actual_cols:
                actual_field = next(
                    f for f in actual_fields
                    if f.name == expected_field.name
                )
                if actual_field.dataType != expected_field.dataType:
                    errors.append(
                        f"Type mismatch for {expected_field.name}: "
                        f"expected {expected_field.dataType}, "
                        f"got {actual_field.dataType}"
                    )
        
        return errors
    
    def _check_nulls(
        self,
        df: DataFrame,
        null_checks: Dict[str, float]
    ) -> List[str]:
        """Check null value percentages."""
        errors = []
        
        for column, threshold in null_checks.items():
            if column not in df.columns:
                continue
                
            null_count = df.filter(
                F.col(column).isNull()
            ).count()
            null_pct = null_count / df.count()
            
            if null_pct > threshold:
                errors.append(
                    f"Column {column} has {null_pct:.2%} null values, "
                    f"exceeding threshold of {threshold:.2%}"
                )
        
        return errors
    
    def _check_types(
        self,
        df: DataFrame,
        type_checks: Dict[str, str]
    ) -> List[str]:
        """Check data type casting."""
        errors = []
        
        for column, expected_type in type_checks.items():
            if column not in df.columns:
                continue
                
            try:
                df.select(
                    F.cast(F.col(column), expected_type)
                ).filter(
                    F.col(column).isNotNull()
                ).first()
            except Exception as e:
                errors.append(
                    f"Column {column} has values that cannot be cast to "
                    f"{expected_type}: {str(e)}"
                )
        
        return errors
    
    def _check_ranges(
        self,
        df: DataFrame,
        range_checks: Dict[str, Dict[str, Any]]
    ) -> List[str]:
        """Check value ranges."""
        errors = []
        
        for column, ranges in range_checks.items():
            if column not in df.columns:
                continue
                
            if 'min' in ranges:
                min_val = ranges['min']
                below_min = df.filter(
                    F.col(column) < min_val
                ).count()
                if below_min > 0:
                    errors.append(
                        f"Column {column} has {below_min} values below "
                        f"minimum {min_val}"
                    )
            
            if 'max' in ranges:
                max_val = ranges['max']
                above_max = df.filter(
                    F.col(column) > max_val
                ).count()
                if above_max > 0:
                    errors.append(
                        f"Column {column} has {above_max} values above "
                        f"maximum {max_val}"
                    )
        
        return errors 