"""
Search engine module
"""

from typing import List, Dict, Optional, Set
import re
from datetime import date

from .models import Book, BookStatus

class SearchEngine:
    """Search engine cho library."""
    
    def __init__(self):
        """Khởi tạo search engine."""
        self.index = {}  # word -> set of book IDs
        self.data = {}   # book ID -> book data
    
    def _tokenize(self, text: str) -> Set[str]:
        """
        Tách text thành các token.
        
        Args:
            text: Text cần tách
            
        Returns:
            Set các token
        """
        if not text:
            return set()
            
        # Convert to lowercase and split
        words = text.lower().split()
        
        # Remove special characters
        words = [re.sub(r'[^\w\s]', '', word) for word in words]
        
        # Remove empty strings and duplicates
        return set(word for word in words if word)
    
    def _index_text(self, book_id: int, text: str):
        """
        Index text cho book.
        
        Args:
            book_id: ID của book
            text: Text cần index
        """
        for token in self._tokenize(text):
            if token not in self.index:
                self.index[token] = set()
            self.index[token].add(book_id)
    
    def index_book(self, book: Book, data: Dict):
        """
        Index book.
        
        Args:
            book: Book cần index
            data: Data của book
        """
        # Store book data
        self.data[book.id] = data
        
        # Index searchable fields
        self._index_text(book.id, book.title)
        self._index_text(book.id, book.description or "")
        self._index_text(book.id, data.get("author_name", ""))
        self._index_text(book.id, data.get("category_name", ""))
        self._index_text(book.id, " ".join(book.tags))
        self._index_text(book.id, book.notes or "")
    
    def update_book(self, book: Book, data: Dict):
        """
        Update book index.
        
        Args:
            book: Book cần update
            data: Data mới của book
        """
        self.delete_book(book.id)
        self.index_book(book, data)
    
    def delete_book(self, book_id: int):
        """
        Xóa book khỏi index.
        
        Args:
            book_id: ID của book cần xóa
        """
        # Remove from data
        if book_id in self.data:
            del self.data[book_id]
        
        # Remove from index
        for token_set in self.index.values():
            token_set.discard(book_id)
    
    def search(
        self,
        query: Optional[str] = None,
        filters: Optional[Dict] = None
    ) -> List[int]:
        """
        Tìm kiếm book.
        
        Args:
            query: Query text
            filters: Các filter (author, category, status, etc.)
            
        Returns:
            List ID các book phù hợp
        """
        # Start with all books
        results = set(self.data.keys())
        
        # Apply text search
        if query:
            query_results = set()
            for token in self._tokenize(query):
                if token in self.index:
                    if not query_results:
                        query_results = self.index[token].copy()
                    else:
                        query_results &= self.index[token]
            results &= query_results
        
        # Apply filters
        if filters:
            filtered_results = set()
            
            for book_id in results:
                book_data = self.data[book_id]
                match = True
                
                for key, value in filters.items():
                    if key == "author_id":
                        if book_data["author_id"] != value:
                            match = False
                            break
                    elif key == "category_id":
                        if book_data["category_id"] != value:
                            match = False
                            break
                    elif key == "status":
                        if book_data["status"] != value:
                            match = False
                            break
                    elif key == "rating":
                        if book_data["rating"] != value:
                            match = False
                            break
                    elif key == "published_before":
                        if not book_data["published_date"]:
                            match = False
                            break
                        book_date = date.fromisoformat(
                            book_data["published_date"]
                        )
                        if book_date >= value:
                            match = False
                            break
                    elif key == "published_after":
                        if not book_data["published_date"]:
                            match = False
                            break
                        book_date = date.fromisoformat(
                            book_data["published_date"]
                        )
                        if book_date <= value:
                            match = False
                            break
                    elif key == "tags":
                        if not set(value).issubset(book_data["tags"]):
                            match = False
                            break
                
                if match:
                    filtered_results.add(book_id)
            
            results = filtered_results
        
        return sorted(results) 