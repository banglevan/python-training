"""
Bài tập 1: Từ điển đơn giản
Yêu cầu:
1. Quản lý từ vựng (thêm, sửa, xóa, tra cứu)
2. Hỗ trợ nhiều nghĩa cho một từ
3. Lưu trữ ví dụ sử dụng
4. Import/Export từ file
"""

from typing import List, Dict, Optional, Set
from dataclasses import dataclass
import json
from pathlib import Path
import re

@dataclass
class WordMeaning:
    """Class đại diện cho nghĩa của từ."""
    definition: str
    examples: List[str]
    synonyms: Set[str]
    
    def validate(self) -> None:
        """
        Kiểm tra tính hợp lệ của nghĩa từ.
        
        Raises:
            ValueError: Nếu dữ liệu không hợp lệ
        """
        if not self.definition or not isinstance(self.definition, str):
            raise ValueError("Nghĩa của từ không hợp lệ")
        if not isinstance(self.examples, list):
            raise ValueError("Ví dụ phải là danh sách")
        if not isinstance(self.synonyms, set):
            raise ValueError("Từ đồng nghĩa phải là tập hợp")

@dataclass
class DictionaryEntry:
    """Class đại diện cho một từ trong từ điển."""
    word: str
    meanings: List[WordMeaning]
    pronunciation: str
    word_type: str  # noun, verb, adj, etc.
    
    def validate(self) -> None:
        """
        Kiểm tra tính hợp lệ của từ.
        
        Raises:
            ValueError: Nếu dữ liệu không hợp lệ
        """
        if not self.word or not isinstance(self.word, str):
            raise ValueError("Từ không hợp lệ")
        if not isinstance(self.meanings, list) or not self.meanings:
            raise ValueError("Phải có ít nhất một nghĩa")
        for meaning in self.meanings:
            meaning.validate()
        if not isinstance(self.pronunciation, str):
            raise ValueError("Phát âm không hợp lệ")
        if not isinstance(self.word_type, str):
            raise ValueError("Loại từ không hợp lệ")

class Dictionary:
    """Class quản lý từ điển."""
    
    def __init__(self, data_file: str = "dictionary.json"):
        """
        Khởi tạo từ điển với file dữ liệu.
        
        Args:
            data_file (str): Đường dẫn file JSON
        """
        self.data_file = Path(data_file)
        self.entries: Dict[str, DictionaryEntry] = {}  # key: word
        self.load_data()

    def add_word(self, entry: DictionaryEntry) -> bool:
        """
        Thêm từ mới vào từ điển.
        
        Args:
            entry (DictionaryEntry): Từ cần thêm
            
        Returns:
            bool: True nếu thêm thành công
        """
        try:
            entry.validate()
            self.entries[entry.word.lower()] = entry
            self.save_data()
            return True
        except ValueError as e:
            print(f"Lỗi: {e}")
            return False

    def update_word(self, word: str, entry: DictionaryEntry) -> bool:
        """
        Cập nhật thông tin của từ.
        
        Args:
            word (str): Từ cần cập nhật
            entry (DictionaryEntry): Thông tin mới
            
        Returns:
            bool: True nếu cập nhật thành công
        """
        word = word.lower()
        if word not in self.entries:
            return False
        try:
            entry.validate()
            self.entries[word] = entry
            self.save_data()
            return True
        except ValueError as e:
            print(f"Lỗi: {e}")
            return False

    def delete_word(self, word: str) -> bool:
        """
        Xóa từ khỏi từ điển.
        
        Args:
            word (str): Từ cần xóa
            
        Returns:
            bool: True nếu xóa thành công
        """
        word = word.lower()
        if word not in self.entries:
            return False
        del self.entries[word]
        self.save_data()
        return True

    def lookup_word(self, word: str) -> Optional[DictionaryEntry]:
        """
        Tra cứu từ trong từ điển.
        
        Args:
            word (str): Từ cần tra
            
        Returns:
            Optional[DictionaryEntry]: Thông tin từ hoặc None
        """
        return self.entries.get(word.lower())

    def search_words(self, pattern: str) -> List[DictionaryEntry]:
        """
        Tìm kiếm từ theo mẫu.
        
        Args:
            pattern (str): Mẫu tìm kiếm (hỗ trợ regex)
            
        Returns:
            List[DictionaryEntry]: Danh sách từ phù hợp
        """
        try:
            regex = re.compile(pattern, re.IGNORECASE)
            return [
                entry for word, entry in self.entries.items()
                if regex.search(word)
            ]
        except re.error:
            return []

    def import_data(self, file_path: str) -> bool:
        """
        Import dữ liệu từ file.
        
        Args:
            file_path (str): Đường dẫn file
            
        Returns:
            bool: True nếu import thành công
        """
        try:
            path = Path(file_path)
            if not path.exists():
                return False
                
            data = json.loads(path.read_text())
            for word_data in data:
                entry = DictionaryEntry(
                    word=word_data["word"],
                    meanings=[
                        WordMeaning(
                            definition=m["definition"],
                            examples=m["examples"],
                            synonyms=set(m["synonyms"])
                        )
                        for m in word_data["meanings"]
                    ],
                    pronunciation=word_data["pronunciation"],
                    word_type=word_data["word_type"]
                )
                self.add_word(entry)
            return True
        except (json.JSONDecodeError, KeyError, TypeError) as e:
            print(f"Lỗi import: {e}")
            return False

    def export_data(self, file_path: str) -> bool:
        """
        Export dữ liệu ra file.
        
        Args:
            file_path (str): Đường dẫn file
            
        Returns:
            bool: True nếu export thành công
        """
        try:
            path = Path(file_path)
            data = [
                {
                    "word": entry.word,
                    "meanings": [
                        {
                            "definition": m.definition,
                            "examples": m.examples,
                            "synonyms": list(m.synonyms)
                        }
                        for m in entry.meanings
                    ],
                    "pronunciation": entry.pronunciation,
                    "word_type": entry.word_type
                }
                for entry in self.entries.values()
            ]
            path.write_text(json.dumps(data, indent=4))
            return True
        except Exception as e:
            print(f"Lỗi export: {e}")
            return False

    def load_data(self) -> None:
        """Đọc dữ liệu từ file JSON."""
        if not self.data_file.exists():
            return
            
        try:
            data = json.loads(self.data_file.read_text())
            self.entries = {
                entry["word"].lower(): DictionaryEntry(
                    word=entry["word"],
                    meanings=[
                        WordMeaning(
                            definition=m["definition"],
                            examples=m["examples"],
                            synonyms=set(m["synonyms"])
                        )
                        for m in entry["meanings"]
                    ],
                    pronunciation=entry["pronunciation"],
                    word_type=entry["word_type"]
                )
                for entry in data
            }
        except (json.JSONDecodeError, KeyError) as e:
            print(f"Lỗi đọc file: {e}")
            self.entries = {}

    def save_data(self) -> None:
        """Lưu dữ liệu vào file JSON."""
        try:
            data = [
                {
                    "word": entry.word,
                    "meanings": [
                        {
                            "definition": m.definition,
                            "examples": m.examples,
                            "synonyms": list(m.synonyms)
                        }
                        for m in entry.meanings
                    ],
                    "pronunciation": entry.pronunciation,
                    "word_type": entry.word_type
                }
                for entry in self.entries.values()
            ]
            self.data_file.write_text(json.dumps(data, indent=4))
        except Exception as e:
            print(f"Lỗi lưu file: {e}")

def main():
    """Chương trình chính."""
    dictionary = Dictionary()

    while True:
        print("\nTừ Điển Đơn Giản")
        print("1. Thêm từ mới")
        print("2. Cập nhật từ")
        print("3. Xóa từ")
        print("4. Tra từ")
        print("5. Tìm kiếm từ")
        print("6. Import dữ liệu")
        print("7. Export dữ liệu")
        print("8. Thoát")

        choice = input("\nChọn chức năng (1-8): ")

        if choice == '8':
            break
        elif choice == '1':
            word = input("Nhập từ: ")
            word_type = input("Loại từ (n/v/adj/...): ")
            pronunciation = input("Phát âm: ")
            
            meanings = []
            while True:
                definition = input("Nghĩa (Enter để kết thúc): ")
                if not definition:
                    break
                    
                examples = []
                while True:
                    example = input("Ví dụ (Enter để kết thúc): ")
                    if not example:
                        break
                    examples.append(example)
                    
                synonyms = set()
                while True:
                    synonym = input("Từ đồng nghĩa (Enter để kết thúc): ")
                    if not synonym:
                        break
                    synonyms.add(synonym)
                    
                meanings.append(WordMeaning(definition, examples, synonyms))
            
            entry = DictionaryEntry(word, meanings, pronunciation, word_type)
            if dictionary.add_word(entry):
                print("Thêm từ thành công!")
            else:
                print("Thêm từ thất bại!")
                
        elif choice == '2':
            word = input("Nhập từ cần cập nhật: ")
            old_entry = dictionary.lookup_word(word)
            if not old_entry:
                print("Không tìm thấy từ!")
                continue
                
            print("\nThông tin hiện tại:")
            print(f"Từ: {old_entry.word}")
            print(f"Loại từ: {old_entry.word_type}")
            print(f"Phát âm: {old_entry.pronunciation}")
            for i, meaning in enumerate(old_entry.meanings, 1):
                print(f"\nNghĩa {i}:")
                print(f"- Định nghĩa: {meaning.definition}")
                print(f"- Ví dụ: {', '.join(meaning.examples)}")
                print(f"- Từ đồng nghĩa: {', '.join(meaning.synonyms)}")
            
            # TODO: Implement update logic
            
        elif choice == '3':
            word = input("Nhập từ cần xóa: ")
            if dictionary.delete_word(word):
                print("Xóa từ thành công!")
            else:
                print("Không tìm thấy từ!")
                
        elif choice == '4':
            word = input("Nhập từ cần tra: ")
            entry = dictionary.lookup_word(word)
            if entry:
                print(f"\nTừ: {entry.word}")
                print(f"Loại từ: {entry.word_type}")
                print(f"Phát âm: {entry.pronunciation}")
                for i, meaning in enumerate(entry.meanings, 1):
                    print(f"\nNghĩa {i}:")
                    print(f"- Định nghĩa: {meaning.definition}")
                    print(f"- Ví dụ: {', '.join(meaning.examples)}")
                    print(f"- Từ đồng nghĩa: {', '.join(meaning.synonyms)}")
            else:
                print("Không tìm thấy từ!")
                
        elif choice == '5':
            pattern = input("Nhập mẫu tìm kiếm: ")
            results = dictionary.search_words(pattern)
            if results:
                print(f"\nTìm thấy {len(results)} kết quả:")
                for entry in results:
                    print(f"- {entry.word} ({entry.word_type})")
            else:
                print("Không tìm thấy kết quả!")
                
        elif choice == '6':
            file_path = input("Nhập đường dẫn file import: ")
            if dictionary.import_data(file_path):
                print("Import dữ liệu thành công!")
            else:
                print("Import dữ liệu thất bại!")
                
        elif choice == '7':
            file_path = input("Nhập đường dẫn file export: ")
            if dictionary.export_data(file_path):
                print("Export dữ liệu thành công!")
            else:
                print("Export dữ liệu thất bại!")
                
        else:
            print("Lựa chọn không hợp lệ!")

if __name__ == "__main__":
    main() 