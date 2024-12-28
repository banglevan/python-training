# Python Security Rules - Service Rule
```text
author: banglv1
email: banglv1@viettel.com.vn
department: Viettel AIC
description: Quy tắc bảo mật cho các dự án AI-Computer Vision
date: 2024-12-23
```
## 1. Quy định chung (General Rules)
### 1.1 Nguyên tắc cơ bản
 - Luôn sử dụng HTTPS thay vì HTTP
 - Validate tất cả input từ người dùng
 - Không lưu thông tin nhạy cảm trong log files
 - Sử dụng các biến môi trường cho configuration nhạy cảm
 - Giới hạn quyền truy cập file system
 - Thường xuyên cập nhật dependencies
 - Implement proper error handling để tránh lộ thông tin hệ thống

### 1.1.1 Phòng chống tấn công
 - Implement rate limiting cho tất cả API endpoints
 - Sử dụng WAF (Web Application Firewall)
 - Thiết lập giới hạn kích thước request body
 - Implement CAPTCHA cho các form đăng nhập/đăng ký
 - Giới hạn số lượng concurrent connections từ một IP
 - Blacklist các IP có hành vi đáng ngờ
 - Implement request timeout để tránh slow HTTP attacks
 - Sử dụng CDN để phòng chống DDoS
 - Monitor và alert bất thường về traffic
 - Implement circuit breaker pattern cho external services

### 1.1.2 Cấu hình bảo mật
```python
# Rate limiting configuration
ATE_LIMIT_CONFIG = {
   "default": "100/hour",
   "login": "5/minute",
   "api": "1000/day"}
# Request size limits
AX_CONTENT_LENGTH = 10 * 1024 * 1024  # 10MB
# Connection timeouts
EQUEST_TIMEOUT = 30  # seconds
LOW_REQUEST_THRESHOLD = 10  # seconds
# Concurrent connection limits
AX_CONNECTIONS_PER_IP = 50
```
### 1.1.3 Monitoring và Alert
- Theo dõi số lượng requests/giây bất thường
- Alert khi có spike trong failed login attempts
- Monitor memory/CPU usage để phát hiện DOS
- Theo dõi response time bất thường
- Log và alert các security events đáng ngờ
- Implement health checks cho tất cả services
- Automatic scaling khi phát hiện tải cao
- Backup plan và disaster recovery plan
### 1.2 Password và Authentication
- Sử dụng strong password policy (ít nhất 12 ký tự, bao gồm chữ hoa, thường, số, ký tự đặc biệt)
- Implement Multi-Factor Authentication (MFA) cho tài khoản admin
- Sử dụng JWT với expiration time
- Implement refresh token mechanism
- Lưu trữ password dưới dạng đã hash với salt
### 1.3 Session Management
### 1.3.1 Cấu hình cơ bản
```python
pp.config.update(
   PERMANENT_SESSION_LIFETIME=timedelta(minutes=30),
   SESSION_COOKIE_SECURE=True,
   SESSION_COOKIE_HTTPONLY=True,
   SESSION_COOKIE_SAMESITE='Strict')

```
### 1.3.2 Quy định về Session
- Session ID phải được tạo ngẫu nhiên và đủ dài (ít nhất 128 bits)
- Tự động invalidate session sau thời gian không hoạt động
- Regenerate session ID sau khi login/logout hoặc thay đổi quyền
- Không lưu sensitive data trong session
- Xóa session khi logout hoặc timeout
- Implement session fixation protection
### 1.3.3 Cookie Security
- Thiết lập secure flag cho tất cả cookies
- Sử dụng HttpOnly flag để prevent XSS
- Implement SameSite=Strict để prevent CSRF
- Đặt expiration time phù hợp cho cookies
- Mã hóa sensitive data trong cookies
- Validate cookie integrity
### 1.3.4 Implementation Example
```python
from flask import Flask, session
from datetime import timedelta
import secrets
def configure_session_security(app):
   # Session configuration
   app.config.update(
       # Session lifetime
       PERMANENT_SESSION_LIFETIME=timedelta(minutes=30),
       
       # Cookie settings
       SESSION_COOKIE_SECURE=True,
       SESSION_COOKIE_HTTPONLY=True,
       SESSION_COOKIE_SAMESITE='Strict',
       
       # Session protection
       SESSION_PROTECTION='strong',
       
       # Use secure random session key
       SECRET_KEY=secrets.token_hex(32)
   )
def regenerate_session():
   # Save important session data
   data = dict(session)
   
   # Clear session
   session.clear()
   
   # Generate new session ID
   session.regenerate()
   
   # Restore data in new session
   for key, value in data.items():
       session[key] = value
def validate_session():
   # Check session age
   if 'created_at' not in session:
       return False
       
   session_age = datetime.now() - session['created_at']
   if session_age > timedelta(minutes=30):
       session.clear()
       return False
       
   # Check user agent consistency
   if session.get('user_agent') != request.user_agent.string:
       session.clear()
       return False
       
   return True
```
### 1.3.5 Session Monitoring
- Log tất cả session creation/destruction events
- Monitor số lượng active sessions
- Alert khi phát hiện bất thường trong session usage
- Track failed session validation attempts
- Monitor session duration và activity patterns
### 1.3.6 Best Practices
- Sử dụng server-side session storage
- Implement session timeout cho inactive users
- Giới hạn số lượng concurrent sessions cho mỗi user
- Validate session data trước khi sử dụng
- Implement proper session cleanup
- Sử dụng session versioning để handle upgrades
## 2. API Security

### 2.1 Authentication & Authorization
- Implement RBAC (Role-Based Access Control)
- OAuth 2.0 cho third-party authentication
- Rate limiting để prevent DOS attacks:
```python
from flask_limiter import Limiter
limiter = Limiter(
app, key_func=get_remote_address,
default_limits=["200 per day", "50 per hour"]
)
```
### 2.2 Request Validation
```python
from pydantic import BaseModel, validator
class APIRequest(BaseModel):
    data: str
@validator('data')
def validate_data(cls, v):
    if len(v) > 1000:
        raise ValueError('Data too long')
    if not v.isalnum():
        raise ValueError('Data contains invalid characters')
    return v
```
### 2.3 Security Headers
```python
@app.after_request
def security_headers(response):
headers = {
'X-Content-Type-Options': 'nosniff',
'X-Frame-Options': 'SAMEORIGIN',
'X-XSS-Protection': '1; mode=block',
'Content-Security-Policy': "default-src 'self'",
'Strict-Transport-Security': 'max-age=31536000; includeSubDomains'
}
for key, value in headers.items():
response.headers[key] = value
return response
```
## 3. Database & File system Security

### 3.1 Connection Security
```python
from sqlalchemy import create_engine
from urllib.parse import quote_plus
def get_secure_db_connection():
    password = quote_plus(os.getenv('DB_PASSWORD'))
    connection_string = f"postgresql://user: {password}@localhost/dbname"
    return create_engine(
                connection_string,
                pool_size=5,
                max_overflow=10,
                pool_timeout=30,
                pool_recycle=1800,
                connect_args={
                "sslmode": "verify-full",
                "sslcert": "/path/to/client-cert.pem",
                "sslkey": "/path/to/client-key.pem",
                "sslrootcert": "/path/to/server-ca.pem",
                }
)
```
### 3.2 Query Security
- Sử dụng prepared statements hoặc ORM
- Implement query timeout
- Encrypt sensitive data
- Regular database backups
- Principle of least privilege cho database users
## 3. Database & File system Security
### 3.1 Connection Security
```python
from sqlalchemy import create_engine
from urllib.parse import quote_plus
def get_secure_db_connection():
   password = quote_plus(os.getenv('DB_PASSWORD'))
   connection_string = f"postgresql://user:{password}@localhost/dbname"
   return create_engine(
               connection_string,
               pool_size=5,
               max_overflow=10,
               pool_timeout=30,
               pool_recycle=1800,
               connect_args={
                   "sslmode": "verify-full",
                   "sslcert": "/path/to/client-cert.pem",
                   "sslkey": "/path/to/client-key.pem",
                   "sslrootcert": "/path/to/server-ca.pem",
               }
   )
```
### 3.2 Query Security
 Sử dụng prepared statements hoặc ORM
 Implement query timeout
 Encrypt sensitive data
 Regular database backups
 Principle of least privilege cho database users
### 3.3 Access Control và Variables
```python
class DatabaseManager:
   # Private variables - không thể truy cập trực tiếp từ bên ngoài
   __connection = None
   __credentials = None
   __pool = None
   
   # Protected variables - chỉ truy cập được trong class và subclasses
   _query_timeout = 30
   _max_connections = 10
   
   # Public variables - có thể truy cập từ bên ngoài
   allowed_tables = ['public_data', 'shared_info']
   read_only_fields = ['created_at', 'updated_at']
    def __init__(self):
       self.__credentials = {
           'user': os.getenv('DB_USER'),
           'password': os.getenv('DB_PASSWORD'),
           'host': os.getenv('DB_HOST'),
           'database': os.getenv('DB_NAME')
       }
       self.__establish_connection()
    def __establish_connection(self):
       """Private method để thiết lập connection"""
       if not self.__connection:
           self.__connection = get_secure_db_connection()
    @property
   def connection_status(self):
       """Public property để kiểm tra trạng thái connection"""
       return bool(self.__connection)
```
### 3.4 Standardized Query Functions
```python
class QueryManager:
   # Định nghĩa các function chuẩn cho mọi query
   
   @staticmethod
   def validate_fields(fields: list, table: str) -> bool:
       """Validate các fields được phép query"""
       allowed_fields = {
           'users': ['id', 'username', 'email', 'created_at'],
           'products': ['id', 'name', 'price', 'status']
       }
       return all(field in allowed_fields.get(table, []) for field in fields)
    @staticmethod
   def sanitize_input(value: str) -> str:
       """Sanitize input để prevent SQL injection"""
       return re.sub(r'[^a-zA-Z0-9_-]', '', value)
    def execute_select(self, table: str, fields: list, conditions: dict = None):
       """Chuẩn hóa câu lệnh SELECT"""
       if not self.validate_fields(fields, table):
           raise SecurityException("Invalid fields requested")
           
       query = select(fields).select_from(table)
       if conditions:
           sanitized_conditions = {
               self.sanitize_input(k): self.sanitize_input(str(v))
               for k, v in conditions.items()
           }
           query = query.where(and_(*[
               getattr(table.c, k) == v
               for k, v in sanitized_conditions.items()
           ]))
       return self.execute_query(query)
    def execute_query(self, query):
       """Thực thi query với các biện pháp bảo mật"""
       try:
           with self.get_connection() as conn:
               # Set timeout
               conn.execution_options(timeout=self._query_timeout)
               # Log query for audit
               self._log_query(query)
               # Execute with parameters
               return conn.execute(query)
       except Exception as e:
           self._log_error(e)
           raise
```
### 3.5 File Operations Security
```python
from pathlib import Path
import os
import logging
from typing import Union, BinaryIO
class SecureFileManager:
   # Private constants
   __ALLOWED_EXTENSIONS = {'txt', 'pdf', 'png', 'jpg', 'jpeg', 'doc', 'docx', 'xls', 'xlsx'}
   __MAX_FILE_SIZE = 10 * 1024 * 1024  # 10MB
   __BLOCKED_CHARS = {'..', '/', '\\', '~', '`', '$', '*', '&', '|', ';', ' '}
   
   # Protected paths
   _base_upload_path = Path('/secure/uploads')
   _temp_path = Path('/secure/temp')
   _backup_path = Path('/secure/backups')
    def __init__(self):
       # Ensure directories exist
       self._base_upload_path.mkdir(parents=True, exist_ok=True)
       self._temp_path.mkdir(parents=True, exist_ok=True)
       self._backup_path.mkdir(parents=True, exist_ok=True)
    @classmethod
   def secure_file_operation(cls, filename: str, operation: str, content: Union[str, bytes] = None) -> Union[str, bytes, None]:
       """Wrapper cho tất cả file operations"""
       try:
           # Validate filename
           if not cls.__is_safe_filename(filename):
               raise SecurityException("Invalid filename or extension")
            # Validate path
           full_path = cls._base_upload_path / filename
           if not full_path.is_relative_to(cls._base_upload_path):
               raise SecurityException("Path traversal detected")
            # Execute operation with backup
           cls.__backup_file(full_path)
           
           if operation == 'read':
               return cls.__read_file(full_path)
           elif operation == 'write':
               return cls.__write_file(full_path, content)
           elif operation == 'delete':
               return cls.__delete_file(full_path)
           else:
               raise SecurityException("Invalid operation")
        except Exception as e:
           cls.__log_file_operation_error(e)
           raise
    @staticmethod
   def __is_safe_filename(filename: str) -> bool:
       """Private method để validate filename và extension"""
       try:
           # Check for dangerous characters
           if any(char in filename for char in SecureFileManager.__BLOCKED_CHARS):
               return False
            # Validate extension
           if '.' not in filename:
               return False
               
           name, ext = filename.rsplit('.', 1)
           if not name or ext.lower() not in SecureFileManager.__ALLOWED_EXTENSIONS:
               return False
            # Additional filename validation
           if len(filename) > 255:  # Max filename length
               return False
            return True
       except Exception:
           return False
    @classmethod
   def __read_file(cls, path: Path) -> Union[str, bytes]:
       """Private method để đọc file an toàn"""
       if not path.exists():
           raise FileNotFoundError(f"File not found: {path}")
           
       if path.stat().st_size > cls.__MAX_FILE_SIZE:
           raise SecurityException("File too large")
           
       # Check if file is binary or text
       is_binary = path.suffix.lower() in {'.pdf', '.png', '.jpg', '.jpeg'}
       
       mode = 'rb' if is_binary else 'r'
       encoding = None if is_binary else 'utf-8'
       
       with open(path, mode=mode, encoding=encoding) as f:
           return f.read()
    @classmethod
   def __write_file(cls, path: Path, content: Union[str, bytes]) -> bool:
       """Private method để ghi file an toàn"""
       if isinstance(content, str):
           mode = 'w'
           encoding = 'utf-8'
       else:
           mode = 'wb'
           encoding = None
           
       # Check content size
       content_size = len(content.encode()) if isinstance(content, str) else len(content)
       if content_size > cls.__MAX_FILE_SIZE:
           raise SecurityException("Content too large")
        with open(path, mode=mode, encoding=encoding) as f:
           f.write(content)
       return True
    @classmethod
   def __backup_file(cls, path: Path) -> None:
       """Private method để backup file trước khi modify"""
       if path.exists():
           backup_name = f"{path.stem}_{int(time.time())}{path.suffix}"
           backup_path = cls._backup_path / backup_name
           import shutil
           shutil.copy2(path, backup_path)
    @classmethod
   def __delete_file(cls, path: Path) -> bool:
       """Private method để xóa file an toàn"""
       if path.exists():
           cls.__backup_file(path)  # Backup trước khi xóa
           path.unlink()
       return True
    @staticmethod
   def __log_file_operation_error(error: Exception) -> None:
       """Private method để log lỗi"""
       logging.error(f"File operation error: {str(error)}", exc_info=True)
# Usage Example:
""
file_manager = SecureFileManager()
# Đọc file
content = file_manager.secure_file_operation('document.pdf', 'read')
# Ghi file
file_manager.secure_file_operation('new_doc.txt', 'write', 'Hello World')
# Xóa file
file_manager.secure_file_operation('old_doc.txt', 'delete')
""
```
❌ KHÔNG BAO GIỜ SỬ DỤNG:
 `open(user_provided_path, 'r').read()`
 `os.path.join(base_dir, user_input)`\
 `Direct string concatenation cho paths`
 `os.system(f"rm {filename}")`
 `shutil.rmtree(user_path)`
✅ LUÔN LUÔN:
- Sử dụng SecureFileManager cho mọi file operation
- Sử dụng biến private để bảo vệ connection, không bị gọi và can thiệp từ bên ngoài
- Validate filename và extension
- Kiểm tra path traversal
- Giới hạn kích thước file
- Backup trước khi modify
- Log mọi file operation
- Sử dụng try-except để handle errors
- Implement file access permissions
### 3.6 Monitoring và Audit
- Log tất cả database queries
- Track file system operations
- Monitor database connections
- Alert về suspicious queries
- Regular security audits
- Backup strategy
- Access pattern analysis
### 3.7 Best Practices
- Không bao giờ expose database credentials trong code
- Sử dụng connection pooling
- Implement query timeout
- Validate tất cả input trước khi query
- Encrypt sensitive data trong database
- Regular security patches và updates
- Principle of least privilege
- Regular backup và disaster recovery testing

## 4. Dangerous Functions và Alternatives
### 4.1 Built-in Functions Nguy Hiểm
#### 4.1.1 Code Execution
KHÔNG SỬ DỤNG:
```python
 Các functions có thể thực thi code tùy ý
eval(user_input)              # Thực thi Python expressions
exec(user_input)             # Thực thi Python code
compile(user_input, '', 'exec') # Compile code từ string
input()                      # Có thể thực thi code trong Python 2.x
```
✅ THAY THẾ BẰNG:
```python
# Sử dụng ast.literal_eval cho parsing data structures
import ast
def safe_eval(expression: str):
   try:
       return ast.literal_eval(expression)
   except (ValueError, SyntaxError):
       raise SecurityException("Invalid expression")
# Sử dụng json cho data serialization
import json
def parse_data(data_string: str):
   return json.loads(data_string)
```
#### 4.1.2 File Operations
 KHÔNG SỬ DỤNG:
```python
open(user_input, 'r')        # Direct file access
_import__(user_input)       # Dynamic imports
globals()[user_input]        # Dynamic attribute access
setattr(obj, user_input)     # Dynamic attribute access
```
✅ THAY THẾ BẰNG:
```python
from pathlib import Path
from typing import Set
class SafeFileAccess:
   ALLOWED_PATHS: Set[Path] = {Path('/safe/path')}
   
   @classmethod
   def safe_open(cls, filename: str, mode: str = 'r'):
       path = Path(filename).resolve()
       if not any(safe_path in path.parents for safe_path in cls.ALLOWED_PATHS):
           raise SecurityException("Access denied")
       return open(path, mode)
    @staticmethod
   def safe_import(module_name: str):
       WHITELIST = {'json', 'csv', 'yaml'}
       if module_name not in WHITELIST:
           raise SecurityException("Module not allowed")
       return __import__(module_name)
```
#### 4.1.3 System Commands
`KHÔNG SỬ DỤNG:`
```python
os.system(command)           # Shell command execution
os.popen(command)           # Shell command execution
subprocess.call(command, shell=True)  # Shell=True là nguy hiểm
subprocess.Popen(command, shell=True) # Shell=True là nguy hiểm
```
✅ THAY THẾ BẰNG:
```python
import subprocess
from shlex import quote
class SafeCommandExecution:
   ALLOWED_COMMANDS = {'ls', 'cat', 'echo'}
   
   @classmethod
   def execute(cls, command: str, args: list) -> str:
       if command not in cls.ALLOWED_COMMANDS:
           raise SecurityException("Command not allowed")
           
       safe_args = [quote(arg) for arg in args]
       result = subprocess.run(
           [command, *safe_args],
           shell=False,
           check=True,
           capture_output=True,
           text=True
       )
       return result.stdout
```
### 4.2 Dangerous Third-party Libraries
#### 4.2.1 Serialization Libraries
 `KHÔNG SỬ DỤNG:`
```python
pickle.loads(data)           # Có thể thực thi code tùy ý
marshal.loads(data)         # Không an toàn cho untrusted data
yaml.load(data)            # Có thể thực thi code tùy ý
```
✅ THAY THẾ BẰNG:
```python
import yaml
import json
def safe_deserialize(data: str, format: str = 'json'):
   if format == 'yaml':
       return yaml.safe_load(data)
   return json.loads(data)
```
### 4.3 Đánh Giá An Toàn của Third-party Libraries
#### 4.3.1 Checklist Đánh Giá
. Kiểm tra độ phổ biến và uy tín:
  - Số lượng stars trên GitHub
  - Số lượng contributors
  - Tần suất updates
  - Số lượng dependent projects
2. Kiểm tra bảo mật:
  - CVE history
  - Security advisories
  - Bug bounty programs
  - Security policy
  ```python
  import requests
  
  def check_package_security(package_name: str) -> dict:
      # Check PyPI safety database
      safety_db_url = f"https://pypi.org/pypi/{package_name}/json"
      response = requests.get(safety_db_url)
      package_info = response.json()
      
      # Check known vulnerabilities
      vulnerabilities = []
      # Implementation details...
      
      return {
          "name": package_name,
          "current_version": package_info["info"]["version"],
          "vulnerabilities": vulnerabilities,
          "last_update": package_info["urls"][0]["upload_time"]
      }
  ```
3. Code Quality Metrics:
  ```python
  def assess_package_quality(package_name: str) -> dict:
      metrics = {
          "has_tests": False,
          "test_coverage": 0,
          "documentation_score": 0,
          "maintenance_score": 0
      }
      # Implementation details...
      return metrics
  ```
#### 4.3.2 Best Practices cho Third-party Libraries
. Version Pinning:
```python
requirements.txt
package==1.2.3  # Pin specific version
package>=1.2.3,<2.0.0  # Pin major version
```
2. Dependency Scanning:
```bash
Scan dependencies for vulnerabilities
safety check
# Update dependencies safely
pip-audit
```
3. Dependency Isolation:
```python
 Use virtual environments
python -m venv venv
source venv/bin/activate
# Use dependency groups
 requirements-dev.txt -r requirements.txt pytest>=6.0.0 slack>=21.0.0
```
4. Monitoring và Updates:
```python
from packaging import version
import pkg_resources
def check_outdated_packages():
   outdated = []
   for dist in pkg_resources.working_set:
       latest = get_latest_version(dist.project_name)
       if version.parse(dist.version) < version.parse(latest):
           outdated.append((dist.project_name, dist.version, latest))
   return outdated
```
### 4.4 Security Guidelines cho Custom Functions
1. Input Validation:
```python
from typing import Any
import validators
class InputValidator:
   @staticmethod
   def validate_input(value: Any, input_type: str) -> bool:
       validators = {
           'email': validators.email,
           'url': validators.url,
           'ipv4': validators.ipv4,
           'domain': validators.domain
       }
       return validators.get(input_type, lambda x: False)(value)
```
2. Output Encoding:
```python
import html
import urllib.parse
class OutputEncoder:
   @staticmethod
   def html_encode(value: str) -> str:
       return html.escape(value)
   
   @staticmethod
   def url_encode(value: str) -> str:
       return urllib.parse.quote(value)
```
3. Error Handling:
```python
lass SecureFunction:
   def __init__(self):
       self.logger = logging.getLogger(__name__)
   
   def execute(self, *args, **kwargs):
       try:
           self._validate_inputs(*args, **kwargs)
           result = self._process(*args, **kwargs)
           return self._sanitize_output(result)
       except Exception as e:
           self.logger.error(f"Error in secure function: {str(e)}")
           raise SecurityException("Operation failed")
```
## 5. Security Scanning Tools

### 5.1 Static Analysis
- Bandit: Quét mã nguồn Python
```bash
bandit -r ./src -f json -o security-report.json
```
- SonarQube: Phân tích chất lượng và bảo mật
- Safety: Kiểm tra dependencies
```bash
safety check --full-report
```
- Pylint với security plugins

### 5.2 Dynamic Analysis
- OWASP ZAP
- Burp Suite
- Penetration testing tools
- API testing tools (Postman security tests)

### 5.3 CI/CD Security
```yaml
# Example GitHub Actions workflow
security:
runs-on: ubuntu-latest
steps:
uses: actions/checkout@v2
name: Security scan
run: |
pip install bandit safety
bandit -r ./src
safety check
```

## 6. Encryption và Hashing
### 6.1 Data Encryption
```python
from cryptography.fernet import Fernet
class Encryptor:
   def __init__(self):
       self.key = os.getenv('ENCRYPTION_KEY')
       self.f = Fernet(self.key)
   
   def encrypt(self, data: str) -> bytes:
       return self.f.encrypt(data.encode())
   
   def decrypt(self, token: bytes) -> str:
       return self.f.decrypt(token).decode()
```
### 6.2 Password Hashing
```python
from passlib.hash import pbkdf2_sha256
def hash_password(password: str) -> str:
   return pbkdf2_sha256.hash(password)
def verify_password(password: str, hash: str) -> bool:
   return pbkdf2_sha256.verify(password, hash)
```
### 6.3 Code Obfuscation và Protection
#### 6.3.1 Bytecode Compilation
```python
 Compile Python files to bytecode
python -m compileall .
 Remove source files after compilation
ind . -name "*.py" -type f -delete
```
#### 6.3.2 Code Obfuscation Tools
```bash
#Using pyarmor
pip install pyarmor
# Protect single script
pyarmor obfuscate script.py
# Protect entire project
pyarmor obfuscate --recursive project/
# Protect with advanced options
pyarmor obfuscate --advanced 2 --restrict 0 script.py
```
#### 6.3.3 Custom Obfuscation Techniques
```python
import base64
import zlib
def obfuscate_string(s: str) -> str:
   """Mã hóa string thành dạng khó đọc"""
   return base64.b85encode(zlib.compress(s.encode())).decode()
def deobfuscate_string(s: str) -> str:
   """Giải mã string đã bị obfuscate"""
   return zlib.decompress(base64.b85decode(s.encode())).decode()
# Example usage
original_code = """
def sensitive_function():
   secret_key = "very_secret_key"
   return secret_key
""
obfuscated = obfuscate_string(original_code)
# Store obfuscated version in source
PROTECTED_CODE = "H#F^d2F!H^&*Gd..." # obfuscated result
# Runtime execution
exec(deobfuscate_string(PROTECTED_CODE))
```
#### 6.3.4 Binary Packaging
```bash
# Using PyInstaller with encryption
pip install pyinstaller
# Create encrypted binary
pyinstaller --key=YOUR_KEY \
          --onefile \
          --noconsole \
          script.py
```
#### 6.3.5 Advanced Protection Strategies
1. Runtime Environment Checks:
```python
class SecurityManager:
   @staticmethod
   def verify_environment():
       """Kiểm tra môi trường thực thi"""
       if debug_enabled():
           raise SecurityException("Debug mode not allowed")
       if detect_virtualization():
           raise SecurityException("Virtual environment detected")
       if detect_debugger():
           raise SecurityException("Debugger detected")
```
2. Anti-Tampering Checks:
```python
import hashlib
import hmac
class CodeIntegrityChecker:
   def __init__(self, secret_key: bytes):
       self.secret_key = secret_key
       self.code_hashes = {}
    def register_code(self, name: str, code: str):
       """Đăng ký hash của code"""
       hash_value = hmac.new(
           self.secret_key,
           code.encode(),
           hashlib.sha256
       ).hexdigest()
       self.code_hashes[name] = hash_value
    def verify_integrity(self, name: str, code: str) -> bool:
       """Kiểm tra tính toàn vẹn của code"""
       if name not in self.code_hashes:
           return False
       current_hash = hmac.new(
           self.secret_key,
           code.encode(),
           hashlib.sha256
       ).hexdigest()
       return hmac.compare_digest(
           current_hash,
           self.code_hashes[name]
       )
```
3. License Management:
```python
from datetime import datetime, timedelta
import jwt
class LicenseManager:
   def __init__(self, secret_key: str):
       self.secret_key = secret_key
    def generate_license(self, user_id: str, duration_days: int) -> str:
       """Tạo license key"""
       expiration = datetime.utcnow() + timedelta(days=duration_days)
       payload = {
           'user_id': user_id,
           'exp': expiration
       }
       return jwt.encode(payload, self.secret_key, algorithm='HS256')
    def verify_license(self, license_key: str) -> bool:
       """Xác thực license key"""
       try:
           payload = jwt.decode(
               license_key,
               self.secret_key,
               algorithms=['HS256']
           )
           return True
       except jwt.ExpiredSignatureError:
           raise SecurityException("License expired")
       except jwt.InvalidTokenError:
           raise SecurityException("Invalid license")
```
### 6.4 Best Practices for Code Protection
1. Layer Protection:
 Kết hợp nhiều phương pháp bảo vệ
 Implement các checks tại nhiều layers khác nhau
 Sử dụng cả static và runtime protection
2. Update Mechanism:
```python
class UpdateManager:
   def check_updates(self):
       """Kiểm tra và áp dụng updates"""
       if self.has_new_version():
           self.download_update()
           self.verify_signature()
           self.apply_update()
```
3. Monitoring:
```python
class SecurityMonitor:
   def monitor_execution(self):
       """Monitor runtime environment"""
       self.check_memory_tampering()
       self.check_file_integrity()
       self.check_network_activity()
```
4. Guidelines:
- Không lưu trữ sensitive data trong code
- Sử dụng strong encryption cho configuration files
- Regular updates cho protection mechanisms
- Implement proper error handling không leak information
- Sử dụng secure random cho các cryptographic operations

### 7.2 Monitoring
- Implement health checks
- Monitor unusual patterns
- Set up alerts for security events
- Regular log analysis
- Performance monitoring để phát hiện DOS attacks
- Các quy tắc này cần được:
    - Cập nhật thường xuyên
    - Review định kỳ
    - Áp dụng trong quá trình code review
    - Đào tạo cho team members
    - Tích hợp vào CI/CD pipeline