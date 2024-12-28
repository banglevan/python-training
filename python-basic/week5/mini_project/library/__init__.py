"""
Personal Library Package
Version: 1.0.0
"""

from .models import Book, Author, Category
from .manager import LibraryManager
from .storage import Storage
from .search import SearchEngine

__version__ = "1.0.0"
__author__ = "Your Name" 