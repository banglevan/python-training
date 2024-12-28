"""
Data models
"""

from dataclasses import dataclass, field
from datetime import date
from typing import List, Optional, Dict
from enum import Enum

class BookStatus(Enum):
    """Trạng thái sách."""
    AVAILABLE = "available"
    BORROWED = "borrowed"
    LOST = "lost"

@dataclass
class Author:
    """Author model."""
    id: int
    name: str
    birth_date: Optional[date] = None
    nationality: Optional[str] = None
    biography: Optional[str] = None
    
    def to_dict(self) -> Dict:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "name": self.name,
            "birth_date": self.birth_date.isoformat() if self.birth_date else None,
            "nationality": self.nationality,
            "biography": self.biography
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'Author':
        """Create from dictionary."""
        if "birth_date" in data and data["birth_date"]:
            data["birth_date"] = date.fromisoformat(data["birth_date"])
        return cls(**data)

@dataclass
class Category:
    """Category model."""
    id: int
    name: str
    description: Optional[str] = None
    parent_id: Optional[int] = None
    
    def to_dict(self) -> Dict:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "parent_id": self.parent_id
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'Category':
        """Create from dictionary."""
        return cls(**data)

@dataclass
class Book:
    """Book model."""
    id: int
    title: str
    author_id: int
    category_id: int
    isbn: Optional[str] = None
    published_date: Optional[date] = None
    description: Optional[str] = None
    status: BookStatus = BookStatus.AVAILABLE
    tags: List[str] = field(default_factory=list)
    rating: Optional[float] = None
    notes: Optional[str] = None
    
    def to_dict(self) -> Dict:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "title": self.title,
            "author_id": self.author_id,
            "category_id": self.category_id,
            "isbn": self.isbn,
            "published_date": (
                self.published_date.isoformat()
                if self.published_date else None
            ),
            "description": self.description,
            "status": self.status.value,
            "tags": self.tags,
            "rating": self.rating,
            "notes": self.notes
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'Book':
        """Create from dictionary."""
        if "published_date" in data and data["published_date"]:
            data["published_date"] = date.fromisoformat(data["published_date"])
        if "status" in data:
            data["status"] = BookStatus(data["status"])
        return cls(**data) 