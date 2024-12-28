"""
Module chứa các hàm xử lý chuỗi.
"""

import re
from typing import List, Dict, Optional
from enum import Enum

class TextCase(Enum):
    """Enum cho các kiểu chữ."""
    LOWER = "lower"
    UPPER = "upper"
    TITLE = "title"
    SENTENCE = "sentence"

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
        str: Văn bản ��ã chuyển
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

def slugify(text: str) -> str:
    """
    Tạo slug từ text.
    
    Args:
        text (str): Text cần tạo slug
        
    Returns:
        str: Slug đã tạo
    """
    # Chuyển về chữ thường
    text = text.lower()
    
    # Thay thế dấu cách bằng gạch ngang
    text = re.sub(r'\s+', '-', text)
    
    # Chỉ giữ lại chữ cái, số và gạch ngang
    text = re.sub(r'[^a-z0-9-]', '', text)
    
    # Xóa gạch ngang thừa
    text = re.sub(r'-+', '-', text)
    
    return text.strip('-')

def word_count(text: str) -> int:
    """Đếm số từ trong text."""
    return len(text.split())

def char_count(text: str, count_spaces: bool = False) -> int:
    """Đếm số ký tự trong text."""
    if count_spaces:
        return len(text)
    return len(text.replace(' ', ''))

def truncate(
    text: str,
    length: int,
    suffix: str = '...'
) -> str:
    """Cắt text theo độ dài cho trước."""
    if len(text) <= length:
        return text
    return text[:length - len(suffix)] + suffix

def extract_emails(text: str) -> List[str]:
    """Trích xuất email từ text."""
    pattern = r'[\w\.-]+@[\w\.-]+\.\w+'
    return re.findall(pattern, text)

def extract_phones(text: str) -> List[str]:
    """Trích xuất số điện thoại từ text."""
    pattern = r'\+?[\d\s-]{10,}'
    return re.findall(pattern, text)

def extract_urls(text: str) -> List[str]:
    """Trích xuất URL từ text."""
    pattern = r'https?://(?:[\w-]+\.)+[\w-]+(?:/[\w-./?%&=]*)?'
    return re.findall(pattern, text) 