"""
Module chứa các hàm xử lý file.
"""

import os
import shutil
from pathlib import Path
from typing import Union, List, Dict, Optional
import json
import csv
import yaml

def read_text(file_path: Union[str, Path]) -> str:
    """Đọc file text."""
    with open(file_path, 'r', encoding='utf-8') as f:
        return f.read()

def write_text(
    file_path: Union[str, Path],
    content: str,
    append: bool = False
) -> bool:
    """Ghi file text."""
    try:
        mode = 'a' if append else 'w'
        with open(file_path, mode, encoding='utf-8') as f:
            f.write(content)
        return True
    except Exception:
        return False

def read_json(file_path: Union[str, Path]) -> Dict:
    """Đọc file JSON."""
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)

def write_json(
    file_path: Union[str, Path],
    data: Dict,
    indent: int = 4
) -> bool:
    """Ghi file JSON."""
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=indent)
        return True
    except Exception:
        return False

def read_csv(
    file_path: Union[str, Path],
    has_header: bool = True
) -> List[List[str]]:
    """Đọc file CSV."""
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        if has_header:
            next(reader)  # Skip header
        return list(reader)

def write_csv(
    file_path: Union[str, Path],
    data: List[List[str]],
    header: Optional[List[str]] = None
) -> bool:
    """Ghi file CSV."""
    try:
        with open(file_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            if header:
                writer.writerow(header)
            writer.writerows(data)
        return True
    except Exception:
        return False

def read_yaml(file_path: Union[str, Path]) -> Dict:
    """Đọc file YAML."""
    with open(file_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

def write_yaml(
    file_path: Union[str, Path],
    data: Dict
) -> bool:
    """Ghi file YAML."""
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            yaml.dump(data, f)
        return True
    except Exception:
        return False

def copy_file(
    src: Union[str, Path],
    dst: Union[str, Path]
) -> bool:
    """Copy file."""
    try:
        shutil.copy2(src, dst)
        return True
    except Exception:
        return False

def move_file(
    src: Union[str, Path],
    dst: Union[str, Path]
) -> bool:
    """Di chuyển file."""
    try:
        shutil.move(src, dst)
        return True
    except Exception:
        return False

def delete_file(file_path: Union[str, Path]) -> bool:
    """Xóa file."""
    try:
        os.remove(file_path)
        return True
    except Exception:
        return False

def create_dir(dir_path: Union[str, Path]) -> bool:
    """Tạo thư mục."""
    try:
        os.makedirs(dir_path, exist_ok=True)
        return True
    except Exception:
        return False

def delete_dir(dir_path: Union[str, Path]) -> bool:
    """Xóa thư mục."""
    try:
        shutil.rmtree(dir_path)
        return True
    except Exception:
        return False

def list_files(
    dir_path: Union[str, Path],
    pattern: str = "*"
) -> List[Path]:
    """Liệt kê files trong thư mục."""
    return list(Path(dir_path).glob(pattern)) 