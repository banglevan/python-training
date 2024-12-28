"""
Data storage module
"""

from typing import Dict, Union
from pathlib import Path
import json
import shutil

from .models import Book, Author, Category

class Storage:
    """Lưu trữ dữ liệu."""
    
    def __init__(self, data_dir: Union[str, Path]):
        """
        Khởi tạo storage.
        
        Args:
            data_dir: Thư mục chứa dữ liệu
        """
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # File paths
        self.books_file = self.data_dir / "books.json"
        self.authors_file = self.data_dir / "authors.json"
        self.categories_file = self.data_dir / "categories.json"
        
        # Create backup dir
        self.backup_dir = self.data_dir / "backup"
        self.backup_dir.mkdir(exist_ok=True)
    
    def _load_json(self, file_path: Path) -> Dict:
        """Load data từ file JSON."""
        if not file_path.exists():
            return {}
            
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            # If error, try to load from backup
            backup_file = self.backup_dir / file_path.name
            if backup_file.exists():
                with open(backup_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            return {}
    
    def _save_json(self, file_path: Path, data: Dict) -> bool:
        """Save data ra file JSON."""
        try:
            # Backup current file if exists
            if file_path.exists():
                shutil.copy2(file_path, self.backup_dir / file_path.name)
            
            # Save new data
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2)
            return True
            
        except Exception:
            return False
    
    def load_books(self) -> Dict[int, Book]:
        """Load books từ file."""
        data = self._load_json(self.books_file)
        return {
            int(id): Book.from_dict(book_data)
            for id, book_data in data.items()
        }
    
    def save_books(self, books: Dict[int, Book]) -> bool:
        """Save books ra file."""
        data = {
            str(id): book.to_dict()
            for id, book in books.items()
        }
        return self._save_json(self.books_file, data)
    
    def load_authors(self) -> Dict[int, Author]:
        """Load authors từ file."""
        data = self._load_json(self.authors_file)
        return {
            int(id): Author.from_dict(author_data)
            for id, author_data in data.items()
        }
    
    def save_authors(self, authors: Dict[int, Author]) -> bool:
        """Save authors ra file."""
        data = {
            str(id): author.to_dict()
            for id, author in authors.items()
        }
        return self._save_json(self.authors_file, data)
    
    def load_categories(self) -> Dict[int, Category]:
        """Load categories từ file."""
        data = self._load_json(self.categories_file)
        return {
            int(id): Category.from_dict(category_data)
            for id, category_data in data.items()
        }
    
    def save_categories(self, categories: Dict[int, Category]) -> bool:
        """Save categories ra file."""
        data = {
            str(id): category.to_dict()
            for id, category in categories.items()
        }
        return self._save_json(self.categories_file, data) 