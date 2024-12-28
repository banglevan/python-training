# Chương Trình Học Python Cơ Bản

## Mục Lục và Nội Dung Chi Tiết

### Tuần 1: Nhập Môn Python
#### Buổi 1: Giới thiệu & Cài đặt
**Lý thuyết:**
- Python là gì và tại sao chọn Python
- Cài đặt Python, IDE (VSCode/PyCharm)
- Cấu trúc chương trình Python cơ bản
- Biến và kiểu dữ liệu cơ bản

**Bài tập:**
1. Hello World nâng cao (nhận input tên, tuổi)
2. Máy tính đơn giản (các phép tính cơ bản)

**Mini Project:** Temperature Converter
- Chuyển đổi giữa Celsius, Fahrenheit, Kelvin
- Lưu lịch sử chuyển đổi
- Giao diện console thân thiện

#### Buổi 2: Cấu trúc điều khiển
**Lý thuyết:**
- Câu lệnh điều kiện (if-elif-else)
- Vòng lặp (for, while)
- Break và continue
- Try-except cơ bản

**Bài tập:**
1. Kiểm tra số nguyên tố
2. Tính tổng dãy số với điều kiện
3. Game đoán số đơn giản

**Mini Project:** Number Games
- Trò chơi đoán số
- Tính toán điểm số
- Nhiều cấp độ khó

### Tuần 2: Cấu Trúc Dữ Liệu
#### Buổi 3: Lists và Tuples
**Lý thuyết:**
- List operations
- List comprehension
- Tuples và unpacking
- Sorting và searching

**Bài tập:**
1. Quản lý danh sách sinh viên
2. Sắp xếp và tìm kiếm
3. Matrix operations

**Mini Project:** Grade Management
- Nhập điểm nhiều môn học
- Tính điểm trung bình
- Xếp loại học lực

#### Buổi 4: Dictionaries và Sets
**Lý thuyết:**
- Dictionary operations
- Sets và operations
- Dictionary comprehension
- Nested structures

**Bài tập:**
1. Từ điển đơn giản
2. Thống kê từ trong văn bản
3. Quản lý thông tin cá nhân

**Mini Project:** Contact Manager
- CRUD operations
- Tìm kiếm contact
- Import/Export data

### Tuần 3: Functions và Modules
#### Buổi 5: Functions
**Lý thuyết:**
- Function definition
- Arguments và parameters
- Return values
- Lambda functions
- Decorators cơ bản

**Bài tập:**
1. Calculator với functions
2. String processing functions
3. Data validation functions

**Mini Project:** Math Toolkit
- Các hàm toán học
- Unit conversion
- Geometric calculations

#### Buổi 6: Modules và Packages
**Lý thuyết:**
- Import modules
- Create modules
- Package structure
- Virtual environments

**Bài tập:**
1. Tạo module utility
2. Sử dụng external packages
3. Package organization

**Mini Project:** Personal Library
- Module quản lý sách
- Import/Export data
- Search và filter

### Tuần 4: OOP Cơ Bản
#### Buổi 7: Classes và Objects
**Lý thuyết:**
- Class definition
- Attributes và methods
- Constructor
- Instance vs Class variables

**Bài tập:**
1. Class Rectangle
2. Bank Account class
3. Student Management

**Mini Project:** Library Management System
- Book và Member classes
- Borrowing system
- Fine calculation

#### Buổi 8: Inheritance và Polymorphism
**Lý thuyết:**
- Inheritance basics
- Method overriding
- Multiple inheritance
- Abstract classes

**Bài tập:**
1. Shape hierarchy
2. Employee management
3. Animal classification

**Mini Project:** E-commerce System
- Product hierarchy
- Shopping cart
- Order processing

### Final Project Options
1. **Task Management System**
   - CRUD operations
   - File persistence
   - User interface
   - Due date tracking

2. **Quiz Application**
   - Multiple choice questions
   - Score tracking
   - Different categories
   - Statistics

3. **Personal Finance Manager**
   - Income/Expense tracking
   - Categories
   - Reports
   - Data visualization

## Yêu Cầu Đánh Giá
1. **Bài tập (40%)**
   - Hoàn thành các bài tập
   - Code style và documentation
   - Xử lý lỗi

2. **Mini Projects (30%)**
   - Functionality
   - Code organization
   - Error handling
   - User interface

3. **Final Project (30%)**
   - Complexity
   - Code quality
   - Documentation
   - Presentation

## Tài Liệu Tham Khảo
1. Python Official Documentation
2. Real Python Tutorials
3. Python Crash Course (Book)
4. Automate the Boring Stuff with Python

# Tuần 1: Nhập Môn Python

## Bài tập cơ bản

### Bài 1: Hello World nâng cao
**Path:** `/week1/exercises/hello_advanced.py`
- Input: Tên và tuổi người dùng
- Output: Lời chào và thông tin chi tiết
- Test: `/week1/exercises/test_hello_advanced.py`

### Bài 2: Máy tính đơn giản
**Path:** `/week1/exercises/simple_calculator.py`
- Input: Hai số và phép tính
- Output: Kết quả phép tính
- Test: `/week1/exercises/test_simple_calculator.py`

### Bài 3: Kiểm tra số nguyên tố
**Path:** `/week1/exercises/prime_checker.py`
- Input: Số cần kiểm tra hoặc khoảng số
- Output: Kết quả kiểm tra và danh sách số nguyên tố
- Test: `/week1/exercises/test_prime_checker.py`

### Bài 4: Tính tổng dãy số
**Path:** `/week1/exercises/sequence_sum.py`
- Input: Dãy số và điều kiện
- Output: Tổng và danh sách số thỏa mãn
- Test: `/week1/exercises/test_sequence_sum.py`

### Bài 5: Game đoán số
**Path:** `/week1/exercises/number_guessing.py`
- Input: Số đoán của người chơi
- Output: Kết quả và gợi ý
- Test: `/week1/exercises/test_number_guessing.py`

## Mini Project

### Temperature Converter
**Path:** `/week1/mini_project/temperature_converter.py`
- Chức năng: Chuyển đổi giữa các đơn vị nhiệt độ
- Lưu lịch sử chuyển đổi
- Test: `/week1/mini_project/test_temperature_converter.py`

## Cấu trúc thư mục
```
week1/
├── README.md
├── exercises/
│   ├── hello_advanced.py
│   ├── test_hello_advanced.py
│   ├── simple_calculator.py
│   ├── test_simple_calculator.py
│   ├── prime_checker.py
│   ├── test_prime_checker.py
│   ├── sequence_sum.py
│   ├── test_sequence_sum.py
│   ├── number_guessing.py
│   └── test_number_guessing.py
└── mini_project/
    ├── temperature_converter.py
    └── test_temperature_converter.py
```

## Tiêu chí đánh giá
- Functionality (40%): Chức năng hoạt động đúng
- Edge Cases (30%): Xử lý trường hợp đặc biệt
- Exception Handling (20%): Xử lý lỗi
- Performance (10%): Hiệu năng

## Hướng dẫn nộp bài
1. Fork repository
2. Clone về máy local
3. Implement các hàm được đánh dấu TODO
4. Chạy tests để kiểm tra
5. Commit và push code
6. Tạo pull request