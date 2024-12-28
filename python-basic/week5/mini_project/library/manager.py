"""
Library manager module
"""

from typing import List, Optional, Dict, Union
from pathlib import Path
import json
from datetime import date

from .models import Book, Author, Category, BookStatus
from .storage import Storage
from .search import SearchEngine

class LibraryManager:
    """Quản lý thư viện."""
    
    def __init__(self, data_dir: Union[str, Path]):
        """
        Khởi tạo manager.
        
        Args:
            data_dir: Thư mục chứa dữ liệu
        """
        self.storage = Storage(data_dir)
        self.search = SearchEngine()
        
        # Load data
        self.books = self.storage.load_books()
        self.authors = self.storage.load_authors()
        self.categories = self.storage.load_categories()
        
        # Build search index
        self._build_search_index()
    
    def _build_search_index(self):
        """Build search index."""
        for book in self.books.values():
            self.search.index_book(book, self._get_book_data(book))
    
    def _get_book_data(self, book: Book) -> Dict:
        """Get full book data including author and category."""
        author = self.authors.get(book.author_id)
        category = self.categories.get(book.category_id)
        
        return {
            **book.to_dict(),
            "author_name": author.name if author else None,
            "category_name": category.name if category else None
        }
    
    def add_book(self, book: Book) -> bool:
        """Thêm sách."""
        if book.id in self.books:
            return False
            
        self.books[book.id] = book
        self.search.index_book(book, self._get_book_data(book))
        return self.storage.save_books(self.books)
    
    def update_book(self, book: Book) -> bool:
        """Cập nhật sách."""
        if book.id not in self.books:
            return False
            
        self.books[book.id] = book
        self.search.update_book(book, self._get_book_data(book))
        return self.storage.save_books(self.books)
    
    def delete_book(self, book_id: int) -> bool:
        """Xóa sách."""
        if book_id not in self.books:
            return False
            
        del self.books[book_id]
        self.search.delete_book(book_id)
        return self.storage.save_books(self.books)
    
    def get_book(self, book_id: int) -> Optional[Book]:
        """Lấy thông tin sách."""
        return self.books.get(book_id)
    
    def search_books(
        self,
        query: Optional[str] = None,
        filters: Optional[Dict] = None
    ) -> List[Book]:
        """
        Tìm kiếm sách.
        
        Args:
            query: Query text
            filters: Các filter (author, category, status, etc.)
        """
        book_ids = self.search.search(query, filters)
        return [self.books[id] for id in book_ids]
    
    def add_author(self, author: Author) -> bool:
        """Thêm tác giả."""
        if author.id in self.authors:
            return False
            
        self.authors[author.id] = author
        return self.storage.save_authors(self.authors)
    
    def update_author(self, author: Author) -> bool:
        """Cập nhật tác giả."""
        if author.id not in self.authors:
            return False
            
        self.authors[author.id] = author
        return self.storage.save_authors(self.authors)
    
    def delete_author(self, author_id: int) -> bool:
        """Xóa tác giả."""
        if author_id not in self.authors:
            return False
            
        del self.authors[author_id]
        return self.storage.save_authors(self.authors)
    
    def get_author(self, author_id: int) -> Optional[Author]:
        """Lấy thông tin tác giả."""
        return self.authors.get(author_id)
    
    def add_category(self, category: Category) -> bool:
        """Thêm danh mục."""
        if category.id in self.categories:
            return False
            
        self.categories[category.id] = category
        return self.storage.save_categories(self.categories)
    
    def update_category(self, category: Category) -> bool:
        """Cập nhật danh mục."""
        if category.id not in self.categories:
            return False
            
        self.categories[category.id] = category
        return self.storage.save_categories(self.categories)
    
    def delete_category(self, category_id: int) -> bool:
        """Xóa danh mục."""
        if category_id not in self.categories:
            return False
            
        del self.categories[category_id]
        return self.storage.save_categories(self.categories)
    
    def get_category(self, category_id: int) -> Optional[Category]:
        """Lấy thông tin danh mục."""
        return self.categories.get(category_id)
    
    def export_data(self, output_file: Union[str, Path]) -> bool:
        """
        Export dữ liệu ra file JSON.
        
        Args:
            output_file: File JSON đích
        """
        data = {
            "books": {
                id: book.to_dict()
                for id, book in self.books.items()
            },
            "authors": {
                id: author.to_dict()
                for id, author in self.authors.items()
            },
            "categories": {
                id: category.to_dict()
                for id, category in self.categories.items()
            }
        }
        
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2)
            return True
        except Exception:
            return False
    
    def import_data(self, input_file: Union[str, Path]) -> bool:
        """
        Import dữ liệu từ file JSON.
        
        Args:
            input_file: File JSON nguồn
        """
        try:
            with open(input_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Import books
            self.books = {
                id: Book.from_dict(book_data)
                for id, book_data in data["books"].items()
            }
            
            # Import authors
            self.authors = {
                id: Author.from_dict(author_data)
                for id, author_data in data["authors"].items()
            }
            
            # Import categories
            self.categories = {
                id: Category.from_dict(category_data)
                for id, category_data in data["categories"].items()
            }
            
            # Save all data
            return all([
                self.storage.save_books(self.books),
                self.storage.save_authors(self.authors),
                self.storage.save_categories(self.categories)
            ])
            
        except Exception:
            return False 