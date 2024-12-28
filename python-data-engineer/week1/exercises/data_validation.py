"""
Data Validation
- Kiểm tra data types
- Xử lý missing values
- Data constraints
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional
from dataclasses import dataclass
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class ColumnValidation:
    """Column validation rules."""
    name: str
    dtype: str
    required: bool = False
    unique: bool = False
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    allowed_values: Optional[List] = None
    regex_pattern: Optional[str] = None

class DataValidator:
    """Data validation class."""
    
    def __init__(
        self,
        validation_rules: List[ColumnValidation]
    ):
        """Initialize validator."""
        self.rules = {
            rule.name: rule 
            for rule in validation_rules
        }
        self.errors = []
    
    def validate_dtype(
        self,
        df: pd.DataFrame,
        column: str,
        expected_type: str
    ) -> bool:
        """Validate data type."""
        try:
            if expected_type == 'numeric':
                df[column] = pd.to_numeric(
                    df[column],
                    errors='raise'
                )
            elif expected_type == 'datetime':
                df[column] = pd.to_datetime(
                    df[column],
                    errors='raise'
                )
            return True
        except Exception as e:
            self.errors.append(
                f"Type validation failed for {column}: {e}"
            )
            return False
    
    def validate_missing(
        self,
        df: pd.DataFrame,
        column: str,
        required: bool
    ) -> bool:
        """Validate missing values."""
        if required and df[column].isnull().any():
            missing_count = df[column].isnull().sum()
            self.errors.append(
                f"Missing values in required column {column}: "
                f"{missing_count} rows"
            )
            return False
        return True
    
    def validate_unique(
        self,
        df: pd.DataFrame,
        column: str
    ) -> bool:
        """Validate uniqueness."""
        if not df[column].is_unique:
            duplicates = (
                df[column].value_counts() > 1
            ).sum()
            self.errors.append(
                f"Duplicate values in column {column}: "
                f"{duplicates} values"
            )
            return False
        return True
    
    def validate_range(
        self,
        df: pd.DataFrame,
        column: str,
        min_value: Optional[float],
        max_value: Optional[float]
    ) -> bool:
        """Validate value range."""
        if min_value is not None:
            if (df[column] < min_value).any():
                self.errors.append(
                    f"Values below minimum in {column}"
                )
                return False
        
        if max_value is not None:
            if (df[column] > max_value).any():
                self.errors.append(
                    f"Values above maximum in {column}"
                )
                return False
        
        return True
    
    def validate_allowed_values(
        self,
        df: pd.DataFrame,
        column: str,
        allowed_values: List
    ) -> bool:
        """Validate allowed values."""
        invalid = ~df[column].isin(allowed_values)
        if invalid.any():
            invalid_values = df[column][invalid].unique()
            self.errors.append(
                f"Invalid values in {column}: {invalid_values}"
            )
            return False
        return True
    
    def validate_regex(
        self,
        df: pd.DataFrame,
        column: str,
        pattern: str
    ) -> bool:
        """Validate regex pattern."""
        invalid = ~df[column].str.match(pattern)
        if invalid.any():
            invalid_count = invalid.sum()
            self.errors.append(
                f"Invalid format in {column}: "
                f"{invalid_count} rows"
            )
            return False
        return True
    
    def validate_dataframe(
        self,
        df: pd.DataFrame
    ) -> Dict[str, bool]:
        """Validate entire dataframe."""
        results = {}
        self.errors = []
        
        for column, rule in self.rules.items():
            if column not in df.columns:
                self.errors.append(
                    f"Missing column: {column}"
                )
                results[column] = False
                continue
            
            # Validate data type
            if not self.validate_dtype(
                df, column, rule.dtype
            ):
                results[column] = False
                continue
            
            # Validate missing values
            if not self.validate_missing(
                df, column, rule.required
            ):
                results[column] = False
                continue
            
            # Validate uniqueness
            if rule.unique and not self.validate_unique(
                df, column
            ):
                results[column] = False
                continue
            
            # Validate range
            if (rule.min_value is not None or 
                rule.max_value is not None):
                if not self.validate_range(
                    df, column,
                    rule.min_value,
                    rule.max_value
                ):
                    results[column] = False
                    continue
            
            # Validate allowed values
            if (rule.allowed_values and 
                not self.validate_allowed_values(
                    df, column,
                    rule.allowed_values
                )):
                results[column] = False
                continue
            
            # Validate regex
            if (rule.regex_pattern and 
                not self.validate_regex(
                    df, column,
                    rule.regex_pattern
                )):
                results[column] = False
                continue
            
            results[column] = True
        
        return results
    
    def get_validation_report(self) -> Dict:
        """Get validation report."""
        return {
            "error_count": len(self.errors),
            "errors": self.errors
        }

def main():
    """Main function."""
    # Example validation rules
    rules = [
        ColumnValidation(
            name="id",
            dtype="numeric",
            required=True,
            unique=True
        ),
        ColumnValidation(
            name="name",
            dtype="str",
            required=True,
            regex_pattern=r"^[A-Za-z\s]+$"
        ),
        ColumnValidation(
            name="age",
            dtype="numeric",
            min_value=0,
            max_value=120
        ),
        ColumnValidation(
            name="status",
            dtype="str",
            allowed_values=["active", "inactive"]
        )
    ]
    
    # Create validator
    validator = DataValidator(rules)
    
    # Example data
    df = pd.DataFrame({
        "id": [1, 2, 2],  # Duplicate ID
        "name": ["John Doe", "Jane123", "Bob"],  # Invalid name
        "age": [25, 150, -1],  # Invalid ages
        "status": ["active", "pending", "inactive"]  # Invalid status
    })
    
    # Validate data
    results = validator.validate_dataframe(df)
    report = validator.get_validation_report()
    
    # Print results
    logger.info("Validation Results:")
    for column, is_valid in results.items():
        logger.info(f"{column}: {'Valid' if is_valid else 'Invalid'}")
    
    logger.info("\nValidation Report:")
    logger.info(f"Total Errors: {report['error_count']}")
    for error in report['errors']:
        logger.info(f"- {error}")

if __name__ == "__main__":
    main() 