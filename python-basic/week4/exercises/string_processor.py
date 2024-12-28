"""
Bài tập 2: String processing functions
Yêu cầu:
1. Các hàm xử lý chuỗi cơ bản
2. Các hàm tìm kiếm và thay thế
3. Các hàm định dạng văn bản
4. Các hàm validate chuỗi
"""

from typing import List, Dict, Optional, Tuple
import re
from dataclasses import dataclass
from enum import Enum

class TextCase(Enum):
    """Enum cho các kiểu chữ."""
    LOWER = "lower"
    UPPER = "upper"
    TITLE = "title"
    SENTENCE = "sentence"

@dataclass
class TextStats:
    """Class chứa thống kê văn bản."""
    char_count: int  # Số ký tự
    word_count: int  # Số từ
    line_count: int  # Số dòng
    sentence_count: int  # Số câu
    avg_word_length: float  # Độ dài từ trung bình
    avg_sentence_length: float  # Độ dài câu trung bình

def normalize_text(text: str) -> str:
    """
    Chuẩn hóa văn bản.
    
    Args:
        text (str): Văn bản cần chuẩn hóa
        
    Returns:
        str: Văn bản đã chuẩn hóa
    """
    # Xóa khoảng trắng thừa
    text = re.sub(r'\s+', ' ', text.strip())
    
    # Chuẩn hóa dấu câu
    text = re.sub(r'\s*([.,!?])\s*', r'\1 ', text)
    text = re.sub(r'\s+([.,!?])(?=\s|$)', r'\1', text)
    
    # Chuẩn hóa ngoặc
    text = re.sub(r'\(\s+', '(', text)
    text = re.sub(r'\s+\)', ')', text)
    
    return text.strip()

def change_case(text: str, case: TextCase) -> str:
    """
    Chuyển đổi kiểu chữ.
    
    Args:
        text (str): Văn bản cần chuyển
        case (TextCase): Kiểu chữ mới
        
    Returns:
        str: Văn bản đã chuyển
    """
    if case == TextCase.LOWER:
        return text.lower()
    elif case == TextCase.UPPER:
        return text.upper()
    elif case == TextCase.TITLE:
        return text.title()
    elif case == TextCase.SENTENCE:
        # Viết hoa chữ đầu câu
        sentences = re.split(r'([.!?]+)', text)
        result = []
        for i in range(0, len(sentences), 2):
            sentence = sentences[i].strip()
            if sentence:
                sentence = sentence[0].upper() + sentence[1:]
            result.append(sentence)
            if i + 1 < len(sentences):
                result.append(sentences[i + 1])
        return ''.join(result)
    return text

def find_replace(
    text: str,
    find: str,
    replace: str,
    case_sensitive: bool = True,
    whole_word: bool = False
) -> Tuple[str, int]:
    """
    Tìm và thay thế text.
    
    Args:
        text (str): Văn bản gốc
        find (str): Chuỗi cần tìm
        replace (str): Chuỗi thay thế
        case_sensitive (bool): Có phân biệt hoa thường
        whole_word (bool): Chỉ thay thế từ hoàn chỉnh
        
    Returns:
        Tuple[str, int]: (Văn bản mới, số lần thay thế)
    """
    if not case_sensitive:
        text_search = text.lower()
        find = find.lower()
    else:
        text_search = text
        
    if whole_word:
        pattern = r'\b' + re.escape(find) + r'\b'
    else:
        pattern = re.escape(find)
        
    if not case_sensitive:
        regex = re.compile(pattern, re.IGNORECASE)
    else:
        regex = re.compile(pattern)
        
    result = []
    count = 0
    last_end = 0
    
    for match in regex.finditer(text_search):
        start, end = match.span()
        result.append(text[last_end:start])
        result.append(replace)
        last_end = end
        count += 1
        
    result.append(text[last_end:])
    return ''.join(result), count

def get_statistics(text: str) -> TextStats:
    """
    Phân tích thống kê văn bản.
    
    Args:
        text (str): Văn bản cần phân tích
        
    Returns:
        TextStats: Kết quả thống kê
    """
    # Đếm ký tự (không tính khoảng trắng)
    char_count = len(re.sub(r'\s', '', text))
    
    # Đếm từ
    words = re.findall(r'\b\w+\b', text)
    word_count = len(words)
    
    # Đếm dòng
    lines = text.split('\n')
    line_count = len(lines)
    
    # Đếm câu
    sentences = re.split(r'[.!?]+', text)
    sentences = [s.strip() for s in sentences if s.strip()]
    sentence_count = len(sentences)
    
    # Tính trung bình
    avg_word_length = (
        sum(len(word) for word in words) / word_count
        if word_count > 0 else 0
    )
    
    avg_sentence_length = (
        word_count / sentence_count
        if sentence_count > 0 else 0
    )
    
    return TextStats(
        char_count=char_count,
        word_count=word_count,
        line_count=line_count,
        sentence_count=sentence_count,
        avg_word_length=avg_word_length,
        avg_sentence_length=avg_sentence_length
    )

def validate_email(email: str) -> bool:
    """
    Kiểm tra email hợp lệ.
    
    Args:
        email (str): Email cần kiểm tra
        
    Returns:
        bool: True nếu hợp lệ
    """
    pattern = r'^[\w\.-]+@[\w\.-]+\.\w+$'
    return bool(re.match(pattern, email))

def validate_phone(phone: str) -> bool:
    """
    Kiểm tra số điện thoại hợp lệ.
    
    Args:
        phone (str): Số điện thoại cần kiểm tra
        
    Returns:
        bool: True nếu hợp lệ
    """
    pattern = r'^\+?[\d\s-]{10,}$'
    return bool(re.match(pattern, phone))

def validate_url(url: str) -> bool:
    """
    Kiểm tra URL hợp lệ.
    
    Args:
        url (str): URL cần kiểm tra
        
    Returns:
        bool: True nếu hợp lệ
    """
    pattern = (
        r'^https?://'  # http:// hoặc https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'  # domain
        r'localhost|'  # localhost
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # IP
        r'(?::\d+)?'  # port
        r'(?:/?|[/?]\S+)$'  # path
    )
    return bool(re.match(pattern, url, re.IGNORECASE))

def main():
    """Chương trình chính."""
    while True:
        print("\nString Processor")
        print("1. Chuẩn hóa văn bản")
        print("2. Chuyển đổi kiểu chữ")
        print("3. Tìm và thay thế")
        print("4. Thống kê văn bản")
        print("5. Kiểm tra định dạng")
        print("6. Thoát")

        choice = input("\nChọn chức năng (1-6): ")

        if choice == '6':
            break
        elif choice == '1':
            text = input("Nhập văn bản: ")
            print("\nKết quả:")
            print(normalize_text(text))
            
        elif choice == '2':
            text = input("Nhập văn bản: ")
            print("\nChọn kiểu chữ:")
            print("1. Chữ thường")
            print("2. Chữ hoa")
            print("3. Viết hoa mỗi từ")
            print("4. Viết hoa đầu câu")
            
            case_choice = input("Chọn kiểu (1-4): ")
            case_map = {
                '1': TextCase.LOWER,
                '2': TextCase.UPPER,
                '3': TextCase.TITLE,
                '4': TextCase.SENTENCE
            }
            
            if case_choice in case_map:
                print("\nKết quả:")
                print(change_case(text, case_map[case_choice]))
            else:
                print("Lựa chọn không hợp lệ!")
                
        elif choice == '3':
            text = input("Nhập văn bản: ")
            find = input("Chuỗi cần tìm: ")
            replace = input("Chuỗi thay thế: ")
            case_sensitive = input("Phân biệt hoa thường? (y/n): ").lower() == 'y'
            whole_word = input("Chỉ thay thế từ hoàn chỉnh? (y/n): ").lower() == 'y'
            
            result, count = find_replace(
                text, find, replace,
                case_sensitive, whole_word
            )
            print(f"\nĐã thay thế {count} lần:")
            print(result)
            
        elif choice == '4':
            text = input("Nhập văn bản: ")
            stats = get_statistics(text)
            
            print("\nThống kê văn bản:")
            print(f"Số ký tự: {stats.char_count}")
            print(f"Số từ: {stats.word_count}")
            print(f"Số dòng: {stats.line_count}")
            print(f"Số câu: {stats.sentence_count}")
            print(f"Độ dài từ trung bình: {stats.avg_word_length:.2f}")
            print(f"Độ dài câu trung bình: {stats.avg_sentence_length:.2f}")
            
        elif choice == '5':
            print("\nChọn kiểu kiểm tra:")
            print("1. Email")
            print("2. Số điện thoại")
            print("3. URL")
            
            type_choice = input("Chọn kiểu (1-3): ")
            value = input("Nhập giá trị cần kiểm tra: ")
            
            if type_choice == '1':
                valid = validate_email(value)
            elif type_choice == '2':
                valid = validate_phone(value)
            elif type_choice == '3':
                valid = validate_url(value)
            else:
                print("Lựa chọn không hợp lệ!")
                continue
                
            print("Hợp lệ!" if valid else "Không hợp lệ!")
            
        else:
            print("Lựa chọn không hợp lệ!")

if __name__ == "__main__":
    main() 