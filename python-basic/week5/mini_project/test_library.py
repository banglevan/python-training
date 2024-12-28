"""
Unit tests cho library package
Tiêu chí đánh giá:
1. Models (20%): Book, Author, Category
2. Storage (20%): Load/Save data
3. Search (30%): Index và search
4. Manager (30%): CRUD và import/export
"""

import pytest
from pathlib import Path
from datetime import date
import json
import shutil

from library.models import Book, Author, Category, BookStatus
from library.storage import Storage
from library.search import SearchEngine
from library.manager import LibraryManager

@pytest.fixture
def temp_dir(tmp_path):
    """Tạo thư mục tạm cho testing."""
    return tmp_path

@pytest.fixture
def sample_data():
    """Tạo sample data cho testing."""
    author = Author(
        id=1,
        name="John Doe",
        birth_date=date(1980, 1, 1),
        nationality="US"
    )
    
    category = Category(
        id=1,
        name="Fiction",
        description="Fiction books"
    )
    
    book = Book(
        id=1,
        title="Sample Book",
        author_id=1,
        category_id=1,
        isbn="1234567890",
        published_date=date(2020, 1, 1),
        description="A sample book",
        status=BookStatus.AVAILABLE,
        tags=["fiction", "sample"],
        rating=4.5
    )
    
    return {
        "author": author,
        "category": category,
        "book": book
    }

class TestModels:
    """
    Test models (20%)
    Pass: Serialize/deserialize đúng ≥ 95%
    Fail: Serialize/deserialize đúng < 95%
    """
    
    def test_author_serialization(self, sample_data):
        """Test serialize/deserialize Author."""
        author = sample_data["author"]
        
        # To dict
        data = author.to_dict()
        assert data["id"] == 1
        assert data["name"] == "John Doe"
        assert data["birth_date"] == "1980-01-01"
        
        # From dict
        new_author = Author.from_dict(data)
        assert new_author.id == author.id
        assert new_author.name == author.name
        assert new_author.birth_date == author.birth_date

    def test_category_serialization(self, sample_data):
        """Test serialize/deserialize Category."""
        category = sample_data["category"]
        
        # To dict
        data = category.to_dict()
        assert data["id"] == 1
        assert data["name"] == "Fiction"
        
        # From dict
        new_category = Category.from_dict(data)
        assert new_category.id == category.id
        assert new_category.name == category.name

    def test_book_serialization(self, sample_data):
        """Test serialize/deserialize Book."""
        book = sample_data["book"]
        
        # To dict
        data = book.to_dict()
        assert data["id"] == 1
        assert data["title"] == "Sample Book"
        assert data["published_date"] == "2020-01-01"
        assert data["status"] == "available"
        
        # From dict
        new_book = Book.from_dict(data)
        assert new_book.id == book.id
        assert new_book.title == book.title
        assert new_book.published_date == book.published_date
        assert new_book.status == book.status

class TestStorage:
    """
    Test storage (20%)
    Pass: Load/save đúng ≥ 95%
    Fail: Load/save đúng < 95%
    """
    
    def test_save_load_books(self, temp_dir, sample_data):
        """Test save/load books."""
        storage = Storage(temp_dir)
        book = sample_data["book"]
        
        # Save
        books = {book.id: book}
        assert storage.save_books(books) is True
        
        # Load
        loaded = storage.load_books()
        assert len(loaded) == 1
        assert loaded[1].title == "Sample Book"

    def test_save_load_authors(self, temp_dir, sample_data):
        """Test save/load authors."""
        storage = Storage(temp_dir)
        author = sample_data["author"]
        
        # Save
        authors = {author.id: author}
        assert storage.save_authors(authors) is True
        
        # Load
        loaded = storage.load_authors()
        assert len(loaded) == 1
        assert loaded[1].name == "John Doe"

    def test_save_load_categories(self, temp_dir, sample_data):
        """Test save/load categories."""
        storage = Storage(temp_dir)
        category = sample_data["category"]
        
        # Save
        categories = {category.id: category}
        assert storage.save_categories(categories) is True
        
        # Load
        loaded = storage.load_categories()
        assert len(loaded) == 1
        assert loaded[1].name == "Fiction"

    def test_backup_restore(self, temp_dir, sample_data):
        """Test backup và restore."""
        storage = Storage(temp_dir)
        book = sample_data["book"]
        
        # Save original
        books = {book.id: book}
        storage.save_books(books)
        
        # Corrupt file
        with open(storage.books_file, 'w') as f:
            f.write("invalid json")
        
        # Should load from backup
        loaded = storage.load_books()
        assert len(loaded) == 1
        assert loaded[1].title == "Sample Book"

class TestSearch:
    """
    Test search (30%)
    Pass: Search đúng ≥ 95%
    Fail: Search đúng < 95%
    """
    
    def test_indexing(self, sample_data):
        """Test indexing."""
        engine = SearchEngine()
        book = sample_data["book"]
        
        # Index book
        engine.index_book(book, {
            "author_name": "John Doe",
            "category_name": "Fiction"
        })
        
        # Check index
        assert "sample" in engine.index
        assert "fiction" in engine.index
        assert "john" in engine.index
        assert book.id in engine.index["sample"]

    def test_search_query(self, sample_data):
        """Test search by query."""
        engine = SearchEngine()
        book = sample_data["book"]
        
        engine.index_book(book, {
            "author_name": "John Doe",
            "category_name": "Fiction"
        })
        
        # Search
        assert engine.search("sample") == [book.id]
        assert engine.search("fiction") == [book.id]
        assert engine.search("john doe") == [book.id]
        assert engine.search("invalid") == []

    def test_search_filters(self, sample_data):
        """Test search với filters."""
        engine = SearchEngine()
        book = sample_data["book"]
        
        engine.index_book(book, book.to_dict())
        
        # Search with filters
        filters = {"status": "available"}
        assert engine.search(filters=filters) == [book.id]
        
        filters = {"status": "borrowed"}
        assert engine.search(filters=filters) == []
        
        filters = {"rating": 4.5}
        assert engine.search(filters=filters) == [book.id]

class TestManager:
    """
    Test manager (30%)
    Pass: CRUD và import/export đúng ≥ 95%
    Fail: CRUD và import/export đúng < 95%
    """
    
    def test_crud_books(self, temp_dir, sample_data):
        """Test CRUD operations cho books."""
        manager = LibraryManager(temp_dir)
        book = sample_data["book"]
        
        # Create
        assert manager.add_book(book) is True
        
        # Read
        loaded = manager.get_book(book.id)
        assert loaded.title == book.title
        
        # Update
        book.title = "Updated Title"
        assert manager.update_book(book) is True
        loaded = manager.get_book(book.id)
        assert loaded.title == "Updated Title"
        
        # Delete
        assert manager.delete_book(book.id) is True
        assert manager.get_book(book.id) is None

    def test_crud_authors(self, temp_dir, sample_data):
        """Test CRUD operations cho authors."""
        manager = LibraryManager(temp_dir)
        author = sample_data["author"]
        
        # Create
        assert manager.add_author(author) is True
        
        # Read
        loaded = manager.get_author(author.id)
        assert loaded.name == author.name
        
        # Update
        author.name = "Updated Name"
        assert manager.update_author(author) is True
        loaded = manager.get_author(author.id)
        assert loaded.name == "Updated Name"
        
        # Delete
        assert manager.delete_author(author.id) is True
        assert manager.get_author(author.id) is None

    def test_search_books(self, temp_dir, sample_data):
        """Test search books."""
        manager = LibraryManager(temp_dir)
        book = sample_data["book"]
        
        # Add book
        manager.add_book(book)
        
        # Search
        results = manager.search_books("sample")
        assert len(results) == 1
        assert results[0].id == book.id
        
        results = manager.search_books(
            filters={"status": "available"}
        )
        assert len(results) == 1
        assert results[0].id == book.id

    def test_import_export(self, temp_dir, sample_data):
        """Test import/export data."""
        manager = LibraryManager(temp_dir)
        
        # Add data
        manager.add_author(sample_data["author"])
        manager.add_category(sample_data["category"])
        manager.add_book(sample_data["book"])
        
        # Export
        export_file = temp_dir / "export.json"
        assert manager.export_data(export_file) is True
        
        # Create new manager
        new_manager = LibraryManager(temp_dir / "new")
        
        # Import
        assert new_manager.import_data(export_file) is True
        
        # Verify data
        book = new_manager.get_book(1)
        assert book.title == "Sample Book"
        
        author = new_manager.get_author(1)
        assert author.name == "John Doe"
        
        category = new_manager.get_category(1)
        assert category.name == "Fiction"