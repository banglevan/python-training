# Chương Trình Đào Tạo Python Cơ Bản

## Quy Ước và Tiêu Chuẩn Code

### 1. Code Style
- Tuân thủ PEP 8
- Sử dụng 4 spaces cho indentation
- Giới hạn 79 ký tự mỗi dòng
- Docstrings cho mọi module, class và function
- Type hints cho parameters và return values

### 2. Cấu Trúc File
```
function_name.py       # Snake case cho tên file
test_function_name.py  # Prefix 'test_' cho test files
```

### 3. Naming Conventions
```python
# Variables và Functions: snake_case
user_name = "Alice"
def calculate_total():
    pass

# Classes: PascalCase
class UserAccount:
    pass

# Constants: UPPERCASE
MAX_ATTEMPTS = 3

# Protected/Private: underscore prefix
_internal_value = 0
__private_method = 0
```

### 4. Documentation
```python
def process_data(input_data: str) -> Dict[str, Any]:
    """
    Mô tả ngắn gọn về function.

    Args:
        input_data (str): Mô tả parameter

    Returns:
        Dict[str, Any]: Mô tả return value

    Raises:
        ValueError: Mô tả khi nào raise exception
    """
    pass
```

### 5. Testing Standards
- Unit tests bắt buộc cho mọi function
- Độ bao phủ tối thiểu 80%
- Test cases phải bao gồm:
  - Functionality (40%)
  - Edge Cases (30%)
  - Exception Handling (20%)
  - Performance (10%)

## Cấu Trúc Chương Trình

### 1. Mỗi Tuần Học
- 2 buổi học (3 giờ/buổi)
- 2-3 bài tập cơ bản
- 1 mini project
- 1 bài quiz

### 2. Đánh Giá
```python
class GradingCriteria:
    """
    Tiêu chí chấm điểm cho mỗi bài tập
    """
    FUNCTIONALITY = 0.4  # Tính năng hoạt động đúng
    EDGE_CASES = 0.3    # Xử lý cases đặc biệt
    EXCEPTIONS = 0.2    # Xử lý lỗi
    PERFORMANCE = 0.1   # Hiệu năng
```

### 3. Yêu Cầu Bài Nộp
1. **Code Structure**
   - Tổ chức file rõ ràng
   - Import statements đúng thứ tự
   - Phân chia function hợp lý

2. **Documentation**
   - Docstrings đầy đủ
   - Comments cho logic phức tạp
   - README cho mini projects

3. **Testing**
   - Unit tests đầy đủ
   - Test cases có ý nghĩa
   - Performance benchmarks

4. **Error Handling**
   - Xử lý exceptions phù hợp
   - Error messages rõ ràng
   - Logging khi cần thiết

## Quy Trình Làm Bài

### 1. Chuẩn Bị
```bash
# Clone repository
git clone <repo_url>

# Tạo virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows

# Cài đặt dependencies
pip install -r requirements.txt
```

### 2. Làm Bài
1. Đọc kỹ yêu cầu và docstrings
2. Implement các functions được đánh dấu TODO
3. Chạy tests để kiểm tra
4. Optimize code nếu cần

### 3. Nộp Bài
```bash
# Kiểm tra code style
pylint your_solution.py

# Chạy tests
pytest test_your_solution.py

# Commit và push
git add .
git commit -m "Submit solution for Exercise X"
git push origin main
```

## Tài Nguyên Học Tập
1. [Python Official Documentation](https://docs.python.org/3/)
2. [Real Python Tutorials](https://realpython.com/)
3. [Python Testing with pytest](https://pytest.org/)
4. [Python Type Hints](https://mypy.readthedocs.io/)

## Hỗ Trợ
- Forum: [Link to Forum]
- Office Hours: [Schedule]
- Email: [Contact Email] 