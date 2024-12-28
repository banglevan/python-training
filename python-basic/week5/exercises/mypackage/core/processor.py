"""
Processor module
"""

from typing import Any, List, Dict, Optional
from ..exceptions import ProcessingError

class Processor:
    """Data processor."""
    
    @staticmethod
    def filter_list(
        data: List[Any],
        condition: callable
    ) -> List[Any]:
        """
        Lọc list theo điều kiện.
        
        Args:
            data: List dữ liệu
            condition: Hàm điều kiện
            
        Returns:
            List đã lọc
        """
        try:
            return [x for x in data if condition(x)]
        except Exception as e:
            raise ProcessingError(f"Filter error: {e}")
    
    @staticmethod
    def map_list(
        data: List[Any],
        transform: callable
    ) -> List[Any]:
        """
        Áp dụng transform cho mỗi phần tử.
        
        Args:
            data: List dữ liệu
            transform: Hàm transform
            
        Returns:
            List đã transform
        """
        try:
            return [transform(x) for x in data]
        except Exception as e:
            raise ProcessingError(f"Map error: {e}")
    
    @staticmethod
    def group_by(
        data: List[Dict],
        key: str
    ) -> Dict[Any, List[Dict]]:
        """
        Nhóm list dict theo key.
        
        Args:
            data: List các dict
            key: Key để nhóm
            
        Returns:
            Dict các nhóm
        """
        try:
            result = {}
            for item in data:
                group = item.get(key)
                if group not in result:
                    result[group] = []
                result[group].append(item)
            return result
        except Exception as e:
            raise ProcessingError(f"Group error: {e}")
    
    @staticmethod
    def sort_by(
        data: List[Dict],
        key: str,
        reverse: bool = False
    ) -> List[Dict]:
        """
        Sắp xếp list dict theo key.
        
        Args:
            data: List các dict
            key: Key để sắp xếp
            reverse: Sắp xếp ngược
            
        Returns:
            List đã sắp xếp
        """
        try:
            return sorted(
                data,
                key=lambda x: x.get(key),
                reverse=reverse
            )
        except Exception as e:
            raise ProcessingError(f"Sort error: {e}") 