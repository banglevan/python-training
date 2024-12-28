"""
Unit tests cho simple_dictionary.py
Tiêu chí đánh giá:
1. Functionality (40%): CRUD và tìm kiếm hoạt động đúng
2. Data Validation (30%): Kiểm tra dữ liệu chặt chẽ
3. Exception Handling (20%): Xử lý lỗi tốt
4. File I/O (10%): Import/Export ổn định
"""

import pytest
from pathlib import Path
import json
from simple_dictionary import (
    WordMeaning,
    DictionaryEntry,
    Dictionary
)

@pytest.fixture
def temp_file(tmp_path):
    """Tạo file tạm cho testing."""
    return tmp_path / "test_dictionary.json"

@pytest.fixture
def sample_meanings():
    """Tạo dữ liệu nghĩa mẫu."""
    return [
        WordMeaning(
            definition="To move quickly",
            examples=["He ran to school", "She runs every morning"],
            synonyms={"sprint", "dash", "jog"}
        ),
        WordMeaning(
            definition="To manage or operate",
            examples=["They run a business", "Running a program"],
            synonyms={"manage", "operate", "control"}
        )
    ]

@pytest.fixture
def sample_entries(sample_meanings):
    """Tạo dữ liệu từ mẫu."""
    return {
        "run": DictionaryEntry(
            word="run",
            meanings=sample_meanings,
            pronunciation="rʌn",
            word_type="verb"
        ),
        "book": DictionaryEntry(
            word="book",
            meanings=[
                WordMeaning(
                    definition="A written work",
                    examples=["Reading a book", "Writing a book"],
                    synonyms={"volume", "text", "publication"}
                )
            ],
            pronunciation="bʊk",
            word_type="noun"
        )
    }

class TestWordMeaning:
    """Test class WordMeaning."""
    
    def test_valid_meaning(self):
        """Test khởi tạo nghĩa hợp lệ."""
        meaning = WordMeaning(
            definition="Test definition",
            examples=["Example 1", "Example 2"],
            synonyms={"syn1", "syn2"}
        )
        meaning.validate()  # Không raise exception

    def test_invalid_meaning(self):
        """Test với dữ liệu không hợp lệ."""
        # Định nghĩa trống
        with pytest.raises(ValueError):
            WordMeaning("", [], set()).validate()
        
        # Examples không phải list
        with pytest.raises(ValueError):
            WordMeaning("Test", "not a list", set()).validate()
        
        # Synonyms không phải set
        with pytest.raises(ValueError):
            WordMeaning("Test", [], "not a set").validate()

class TestDictionaryEntry:
    """Test class DictionaryEntry."""
    
    def test_valid_entry(self, sample_meanings):
        """Test khởi tạo từ hợp lệ."""
        entry = DictionaryEntry(
            word="test",
            meanings=sample_meanings,
            pronunciation="test",
            word_type="noun"
        )
        entry.validate()  # Không raise exception

    def test_invalid_entry(self, sample_meanings):
        """Test với dữ liệu không hợp lệ."""
        # Từ trống
        with pytest.raises(ValueError):
            DictionaryEntry(
                word="",
                meanings=sample_meanings,
                pronunciation="test",
                word_type="noun"
            ).validate()
        
        # Không có nghĩa
        with pytest.raises(ValueError):
            DictionaryEntry(
                word="test",
                meanings=[],
                pronunciation="test",
                word_type="noun"
            ).validate()

class TestDictionary:
    """Test class Dictionary."""
    
    class TestFunctionality:
        """
        Test chức năng cơ bản (40%)
        Pass: Tất cả operations hoạt động đúng
        Fail: Có operation không hoạt động đúng
        """
        
        def test_add_word(self, temp_file, sample_entries):
            """Test thêm từ."""
            dictionary = Dictionary(temp_file)
            
            # Thêm từ mới
            entry = sample_entries["run"]
            assert dictionary.add_word(entry) is True
            assert "run" in dictionary.entries
            
            # Thêm từ đã tồn tại (ghi đè)
            new_entry = DictionaryEntry(
                word="run",
                meanings=[sample_entries["run"].meanings[0]],  # Chỉ lấy nghĩa đầu
                pronunciation="rʌn",
                word_type="verb"
            )
            assert dictionary.add_word(new_entry) is True
            assert len(dictionary.entries["run"].meanings) == 1

        def test_update_word(self, temp_file, sample_entries):
            """Test cập nhật từ."""
            dictionary = Dictionary(temp_file)
            dictionary.add_word(sample_entries["run"])
            
            # Cập nhật từ tồn tại
            updated_entry = DictionaryEntry(
                word="run",
                meanings=[
                    WordMeaning(
                        definition="New definition",
                        examples=["New example"],
                        synonyms={"new"}
                    )
                ],
                pronunciation="new",
                word_type="new"
            )
            assert dictionary.update_word("run", updated_entry) is True
            assert dictionary.entries["run"].meanings[0].definition == "New definition"
            
            # Cập nhật từ không tồn tại
            assert dictionary.update_word("nonexistent", updated_entry) is False

        def test_delete_word(self, temp_file, sample_entries):
            """Test xóa từ."""
            dictionary = Dictionary(temp_file)
            dictionary.add_word(sample_entries["run"])
            
            # Xóa từ tồn tại
            assert dictionary.delete_word("run") is True
            assert "run" not in dictionary.entries
            
            # Xóa từ không tồn tại
            assert dictionary.delete_word("nonexistent") is False

        def test_lookup_word(self, temp_file, sample_entries):
            """Test tra cứu từ."""
            dictionary = Dictionary(temp_file)
            dictionary.add_word(sample_entries["run"])
            
            # Tra từ tồn tại
            entry = dictionary.lookup_word("run")
            assert entry is not None
            assert entry.word == "run"
            
            # Tra từ không tồn tại
            assert dictionary.lookup_word("nonexistent") is None

        def test_search_words(self, temp_file, sample_entries):
            """Test tìm kiếm từ."""
            dictionary = Dictionary(temp_file)
            for entry in sample_entries.values():
                dictionary.add_word(entry)
            
            # Tìm theo pattern
            results = dictionary.search_words("r.n")
            assert len(results) == 1
            assert results[0].word == "run"
            
            # Tìm không có kết quả
            assert len(dictionary.search_words("xyz")) == 0
            
            # Pattern không hợp lệ
            assert len(dictionary.search_words("[")) == 0

    class TestDataValidation:
        """
        Test kiểm tra dữ liệu (30%)
        Pass: Validate đúng ≥ 90% cases
        Fail: Validate đúng < 90% cases
        """
        
        def test_word_validation(self, temp_file):
            """Test validate từ."""
            dictionary = Dictionary(temp_file)
            
            invalid_entries = [
                # Từ trống
                DictionaryEntry(
                    word="",
                    meanings=[
                        WordMeaning(
                            definition="Test",
                            examples=["Test"],
                            synonyms={"test"}
                        )
                    ],
                    pronunciation="test",
                    word_type="noun"
                ),
                # Không có nghĩa
                DictionaryEntry(
                    word="test",
                    meanings=[],
                    pronunciation="test",
                    word_type="noun"
                ),
                # Nghĩa không hợp lệ
                DictionaryEntry(
                    word="test",
                    meanings=[
                        WordMeaning(
                            definition="",
                            examples=[],
                            synonyms=set()
                        )
                    ],
                    pronunciation="test",
                    word_type="noun"
                )
            ]
            
            for entry in invalid_entries:
                assert dictionary.add_word(entry) is False

    class TestExceptionHandling:
        """
        Test xử lý ngoại lệ (20%)
        Pass: Xử lý đúng tất cả exceptions
        Fail: Có exception không được xử lý
        """
        
        def test_file_operations(self, tmp_path):
            """Test xử lý lỗi file."""
            # File trong thư mục không tồn tại
            invalid_path = tmp_path / "nonexistent" / "dict.json"
            dictionary = Dictionary(invalid_path)
            assert len(dictionary.entries) == 0
            
            # File không có quyền ghi
            readonly_file = tmp_path / "readonly.json"
            readonly_file.touch()
            readonly_file.chmod(0o444)  # Read-only
            dictionary = Dictionary(readonly_file)
            dictionary.add_word(
                DictionaryEntry(
                    word="test",
                    meanings=[
                        WordMeaning(
                            definition="Test",
                            examples=["Test"],
                            synonyms={"test"}
                        )
                    ],
                    pronunciation="test",
                    word_type="noun"
                )
            )
            dictionary.save_data()  # Không raise exception

        def test_corrupted_data(self, temp_file):
            """Test với file dữ liệu hỏng."""
            # Tạo file JSON không hợp lệ
            temp_file.write_text("invalid json")
            dictionary = Dictionary(temp_file)
            assert len(dictionary.entries) == 0

    class TestFileIO:
        """
        Test Import/Export (10%)
        Pass: Dữ liệu được lưu và đọc chính xác
        Fail: Dữ liệu bị mất hoặc sai lệch
        """
        
        def test_import_export(self, temp_file, tmp_path, sample_entries):
            """Test import/export dữ liệu."""
            dictionary = Dictionary(temp_file)
            
            # Tạo file export
            export_file = tmp_path / "export.json"
            for entry in sample_entries.values():
                dictionary.add_word(entry)
            assert dictionary.export_data(str(export_file)) is True
            
            # Import vào từ điển mới
            new_dict = Dictionary(tmp_path / "new.json")
            assert new_dict.import_data(str(export_file)) is True
            
            # Kiểm tra dữ liệu
            assert len(new_dict.entries) == len(sample_entries)
            for word, entry in sample_entries.items():
                assert word in new_dict.entries
                assert new_dict.entries[word].word == entry.word
                assert len(new_dict.entries[word].meanings) == len(entry.meanings)

        def test_data_persistence(self, temp_file, sample_entries):
            """Test lưu và đọc dữ liệu."""
            # Lưu dữ liệu
            dict1 = Dictionary(temp_file)
            for entry in sample_entries.values():
                dict1.add_word(entry)
            dict1.save_data()
            
            # Đọc và kiểm tra
            dict2 = Dictionary(temp_file)
            assert len(dict2.entries) == len(sample_entries)
            for word, entry in sample_entries.items():
                assert word in dict2.entries
                assert dict2.entries[word].word == entry.word
                assert len(dict2.entries[word].meanings) == len(entry.meanings) 