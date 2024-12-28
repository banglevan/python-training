"""
Converter module
"""

from typing import Union, Dict
from enum import Enum
from ..exceptions import ConversionError

class Unit(Enum):
    """Base units."""
    METER = "m"
    KILOGRAM = "kg"
    SECOND = "s"
    KELVIN = "K"

class Converter:
    """Unit converter."""
    
    # Conversion factors to base units
    LENGTH_FACTORS = {
        "m": 1,
        "km": 1000,
        "cm": 0.01,
        "mm": 0.001,
        "in": 0.0254,
        "ft": 0.3048,
        "yd": 0.9144,
        "mi": 1609.344
    }
    
    MASS_FACTORS = {
        "kg": 1,
        "g": 0.001,
        "mg": 0.000001,
        "lb": 0.45359237,
        "oz": 0.028349523125
    }
    
    TEMPERATURE_OFFSETS = {
        "K": (0, 1),
        "C": (273.15, 1),
        "F": (255.372, 5/9)
    }
    
    @classmethod
    def convert_length(
        cls,
        value: float,
        from_unit: str,
        to_unit: str
    ) -> float:
        """Convert length."""
        if from_unit not in cls.LENGTH_FACTORS:
            raise ConversionError(f"Invalid unit: {from_unit}")
        if to_unit not in cls.LENGTH_FACTORS:
            raise ConversionError(f"Invalid unit: {to_unit}")
            
        # Convert to base unit (meters)
        base = value * cls.LENGTH_FACTORS[from_unit]
        
        # Convert to target unit
        return base / cls.LENGTH_FACTORS[to_unit]
    
    @classmethod
    def convert_mass(
        cls,
        value: float,
        from_unit: str,
        to_unit: str
    ) -> float:
        """Convert mass."""
        if from_unit not in cls.MASS_FACTORS:
            raise ConversionError(f"Invalid unit: {from_unit}")
        if to_unit not in cls.MASS_FACTORS:
            raise ConversionError(f"Invalid unit: {to_unit}")
            
        # Convert to base unit (kilograms)
        base = value * cls.MASS_FACTORS[from_unit]
        
        # Convert to target unit
        return base / cls.MASS_FACTORS[to_unit]
    
    @classmethod
    def convert_temperature(
        cls,
        value: float,
        from_unit: str,
        to_unit: str
    ) -> float:
        """Convert temperature."""
        if from_unit not in cls.TEMPERATURE_OFFSETS:
            raise ConversionError(f"Invalid unit: {from_unit}")
        if to_unit not in cls.TEMPERATURE_OFFSETS:
            raise ConversionError(f"Invalid unit: {to_unit}")
            
        # Convert to Kelvin
        from_offset, from_factor = cls.TEMPERATURE_OFFSETS[from_unit]
        kelvin = (value + from_offset) * from_factor
        
        # Convert to target unit
        to_offset, to_factor = cls.TEMPERATURE_OFFSETS[to_unit]
        return kelvin / to_factor - to_offset 