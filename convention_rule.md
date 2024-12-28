# Quy tắc Convention Python (Dựa trên PEP 8)
```text
tham khảo PEP 8: https://www.python.org/dev/peps/pep-0008/
author: banglv1
email: banglv1@viettel.com.vn
department: Viettel AIC
description: Quy tắc convention Python cho các dự án AI-Computer Vision
date: 2024-12-23
```
## 1. Bố cục Code Chung
### 1.0 Kết cấu project
```text
project_root/
├── data/
│   ├── raw/                    # Dữ liệu thô
│   ├── processed/              # Dữ liệu đã xử lý
│   ├── models/                 # Model đã train
│   └── test_data/             # Dữ liệu test
├── src/
│   ├── preprocessing/          # Xử lý dữ liệu
│   │   ├── __init__.py
│   │   ├── image_processor.py
│   │   └── video_processor.py
│   ├── models/                # Model definitions
│   │   ├── __init__.py
│   │   ├── cnn_model.py
│   │   └── transformer.py
│   └── evaluation/            # Đánh giá model
│       ├── __init__.py
│       ├── metrics.py
│       └── visualizer.py
├── tests/                     # Test scripts
│   ├── unit/                 # Unit tests
│   │   ├── test_preprocessing.py
│   │   ├── test_models.py
│   │   └── test_evaluation.py
│   ├── integration/          # Integration tests
│   │   ├── test_pipeline.py
│   │   └── test_api.py
│   ├── performance/         # Performance tests
│   │   ├── test_speed.py
│   │   └── test_memory.py
│   └── conftest.py         # Pytest configurations
├── configs/                  # Configuration files
│   ├── model_config.yaml
│   ├── pipeline_config.yaml
│   └── test_config.yaml
├── scripts/                  # Utility scripts
│   ├── train.py
│   ├── evaluate.py
│   └── run_tests.sh
├── documentation/            # Documentation files
│   ├── user_guide.md
│   ├── technical_spec.md
│   └── deployment_guide.md
├── requirements.txt          # Project dependencies
├── setup.py                 # Package setup
├── README.md                # Project documentation
└── .gitignore              # Git ignore rules
```

### 1.0.1 Unit Test
#### 1. Cấu trúc Tests
```python
# tests/unit/test_preprocessing.py
import pytest
import numpy as np
from src.preprocessing.image_processor import ImageProcessor

class TestImageProcessor:
    @pytest.fixture
    def processor(self):
        return ImageProcessor()
        
    @pytest.fixture
    def sample_image(self):
        return np.random.rand(224, 224, 3)
        
    def test_resize(self, processor, sample_image):
        target_size = (112, 112)
        resized = processor.resize(sample_image, target_size)
        assert resized.shape == (*target_size, 3)
        
    def test_normalize(self, processor, sample_image):
        normalized = processor.normalize(sample_image)
        assert normalized.max() <= 1.0
        assert normalized.min() >= 0.0

# tests/integration/test_pipeline.py
class TestPipeline:
    @pytest.fixture
    def pipeline(self):
        return DataPipeline(config_path='configs/test_config.yaml')
        
    def test_end_to_end(self, pipeline):
        input_data = load_test_data()
        result = pipeline.run(input_data)
        assert validate_output(result)

# tests/performance/test_speed.py
class TestPerformance:
    def test_processing_speed(self):
        with timing_context() as timer:
            process_batch(large_test_batch)
        assert timer.elapsed < SPEED_THRESHOLD
```

#### 2. Test Configurations
```yaml
# configs/test_config.yaml
test:
  unit:
    image_size: [224, 224]
    batch_size: 32
    tolerance: 1e-6
    
  integration:
    data_path: "data/test_data"
    timeout: 300
    
  performance:
    speed_threshold: 10.0  # seconds
    memory_limit: 4096  # MB
    batch_sizes: [1, 8, 16, 32, 64]
```

#### 3. Test Execution Script
```bash
#!/bin/bash
# scripts/run_tests.sh

# Run unit tests
echo "Running unit tests..."
pytest tests/unit -v

# Run integration tests
echo "Running integration tests..."
pytest tests/integration -v

# Run performance tests
echo "Running performance tests..."
pytest tests/performance -v --durations=0

# Generate coverage report
pytest --cov=src tests/ --cov-report=html
```
### 1.0.2 Các quy định về nguyên tắc module không phụ thuộc, các hàm độc lập, tạo nên các lib có thể tái tận dụng
### 1.0.4 Các quy định về nguyên tắc cấu trúc module
#### 1. Nguyên tắc SOLID cho Module
```python
 1. Single Responsibility Principle (SRP)
# Mỗi module chỉ nên có một trách nhiệm duy nhất
class ImageProcessor:
   def process(self, image: np.ndarray) -> np.ndarray:
       pass
class ImageSaver:
   def save(self, image: np.ndarray, path: str) -> None:
       pass
# KHÔNG NÊN:
class ImageHandler:  # Vi phạm SRP - quá nhiều trách nhiệm
   def process(self, image: np.ndarray) -> np.ndarray:
       pass
   def save(self, image: np.ndarray, path: str) -> None:
       pass
   def display(self, image: np.ndarray) -> None:
       pass
```
#### 2. Dependency Injection và Inversion
```python
# Tốt - Sử dụng dependency injection
class ModelTrainer:
   def __init__(self, model: BaseModel, optimizer: BaseOptimizer):
       self.model = model
       self.optimizer = optimizer
    def train(self, data: Dataset) -> None:
       pass
# Không tốt - Hard-coded dependencies
class ModelTrainer:
   def __init__(self):
       self.model = SpecificModel()  # Hard-coded dependency
       self.optimizer = SGD()        # Hard-coded dependency
```
#### 3. Interface Segregation
```python
from abc import ABC, abstractmethod
# Tốt - Các interface nhỏ, chuyên biệt
class DataLoader(ABC):
   @abstractmethod
   def load(self) -> np.ndarray:
       pass
class DataSaver(ABC):
   @abstractmethod
   def save(self, data: np.ndarray) -> None:
       pass
# Không tốt - Interface quá lớn
class DataHandler(ABC):
   @abstractmethod
   def load(self) -> np.ndarray:
       pass
   
   @abstractmethod
   def save(self, data: np.ndarray) -> None:
       pass
   
   @abstractmethod
   def process(self, data: np.ndarray) -> np.ndarray:
       pass
```
#### 4. Modular Design Patterns
```python
# Factory Pattern cho module creation
class ModelFactory:
   @staticmethod
   def create_model(model_type: str) -> BaseModel:
       if model_type == "cnn":
           return CNNModel()
       elif model_type == "transformer":
           return TransformerModel()
       raise ValueError(f"Unknown model type: {model_type}")
# Strategy Pattern cho algorithm selection
class ProcessingStrategy(ABC):
   @abstractmethod
   def process(self, data: np.ndarray) -> np.ndarray:
       pass
class ImageProcessor:
   def __init__(self, strategy: ProcessingStrategy):
       self.strategy = strategy
    def process(self, data: np.ndarray) -> np.ndarray:
       return self.strategy.process(data)
```
#### 5. Reusable Components
```python
# Generic Data Transformer
class DataTransformer(Generic[T]):
   def __init__(self, transforms: List[Callable[[T], T]]):
       self.transforms = transforms
    def transform(self, data: T) -> T:
       for t in self.transforms:
           data = t(data)
       return data
# Reusable Decorators
def validate_input(func):
   @wraps(func)
   def wrapper(self, data: np.ndarray, *args, **kwargs):
       if not isinstance(data, np.ndarray):
           raise TypeError("Input must be numpy array")
       if data.size == 0:
           raise ValueError("Input array is empty")
       return func(self, data, *args, **kwargs)
   return wrapper
```
#### 6. Configuration Management
```python
 Configuration class with validation
@dataclass
class ModelConfig:
   input_size: Tuple[int, int]
   num_layers: int
   hidden_size: int
   
   def __post_init__(self):
       if any(s <= 0 for s in self.input_size):
           raise ValueError("Input size must be positive")
       if self.num_layers <= 0:
           raise ValueError("Number of layers must be positive")
# Configuration loader
class ConfigLoader:
   @staticmethod
   def load(path: str) -> Dict[str, Any]:
       with open(path) as f:
           config = yaml.safe_load(f)
       return config
```
#### 7. Error Handling và Logging
```python
 Centralized error handling
class ErrorHandler:
   def __init__(self, logger: logging.Logger):
       self.logger = logger
    def handle(self, error: Exception, context: str = None):
       error_id = str(uuid.uuid4())
       self.logger.error(
           f"Error ID: {error_id}, Context: {context}, "
           f"Error: {str(error)}", exc_info=True
       )
       return error_id
# Module với error handling
class SafeProcessor:
   def __init__(self, error_handler: ErrorHandler):
       self.error_handler = error_handler
    def process(self, data: np.ndarray) -> Optional[np.ndarray]:
       try:
           return self._process_impl(data)
       except Exception as e:
           self.error_handler.handle(e, "data_processing")
           return None
```
#### 8. Testing Support
```python
 Testable module design
class DataProcessor:
   def __init__(self, validator: Optional[Validator] = None):
       self.validator = validator or DefaultValidator()
       self._processed_count = 0  # For testing/monitoring
    @property
   def processed_count(self) -> int:
       return self._processed_count
    def process(self, data: np.ndarray) -> np.ndarray:
       if self.validator.validate(data):
           self._processed_count += 1
           return self._process_impl(data)
       raise ValidationError("Invalid data")
```
#### 9. Best Practices
1. **Dependency Management**:
  - Sử dụng dependency injection
  - Tránh circular dependencies
  - Minimize external dependencies
2. **Module Interface**:
  - Clear và well-documented APIs
  - Strong type hints
  - Consistent error handling
3. **Configuration**:
  - Externalize configuration
  - Version control cho configs
  - Environment-specific configs
4. **Testing**:
  - Unit tests cho mỗi module
  - Integration tests cho module combinations
  - Mocking external dependencies
5. **Documentation**:
  - API documentation
  - Usage examples
  - Dependencies và requirements

### 1.1 Quy tắc chung khác
#### 1. Thụt lề và Định dạng
```python
# 1.1 Thụt lề cơ bản
def function_name():
   # 4 khoảng trắng cho mỗi cấp
   first_level = True
   if first_level:
       # 8 khoảng trắng cho cấp 2
       second_level = True
# 1.2 Căn chỉnh tham số dài
def long_function_name(
       param1: str,  # căn theo dấu ngoặc đơn
       param2: int,
       param3: Dict[str, Any]) -> bool:
   return True
# 1.3 Căn chỉnh toán tử
total = (first_variable
        + second_variable
        - third_variable)
# 1.4 List/Dict nhiều dòng
my_list = [
   1, 2, 3,
   4, 5, 6,]

my_dict = {
   'key1': 'value1',
   'key2': 'value2',
}
```
#### 2. Quy tắc đặt tên
```python
# 2.1 Class Names: PascalCase
class UserAccount:
   pass
class HTTPRequest:  # Abbreviations giữ nguyên hoa
   pass
# 2.2 Function/Method Names: snake_case
def calculate_total():
   pass
def get_user_by_id(user_id: int):
   pass
# 2.3 Variables: snake_case
user_name = "John"
total_count = 0
# 2.4 Constants: SCREAMING_SNAKE_CASE
MAX_CONNECTIONS = 100
DEFAULT_TIMEOUT = 30
# 2.5 Protected/Private Members
class MyClass:
   def __init__(self):
       self.public_var = 1       # Public
       self._protected_var = 2   # Protected (convention)
       self.__private_var = 3    # Private (name mangling)
   
   def public_method(self):
       pass
   
   def _protected_method(self):  # Internal use
       pass
   
   def __private_method(self):   # Strictly private
       pass
```
#### 3. Imports và Module Organization
```python
# 3.1 Import Order
# 1. Standard library
import os
import sys
from typing import Dict, List, Optional, Union
# 2. Third-party libraries (một dòng trống phân cách)
import numpy as np
import pandas as pd
import requests
# 3. Local application imports (một dòng trống phân cách)
from .models import User
from .utils.helpers import format_date
# 3.2 Import Style
# Tốt
from collections import defaultdict, Counter
from typing import List, Optional
# Không tốt
from collections import *  # Wildcard imports
from module import function1, function2, function3, function4, function5  # Quá nhiều imports
# 3.3 Module Level Dunder Names
"""Module docstring."""
__all__ = ['public_function', 'PublicClass']
_version__ = '0.1.0'
_author__ = 'Author Name'
# Rest of the code
```   
#### 4. Docstrings và Comments
```python
def complex_function(param1: str, param2: int) -> bool:
   """Mô tả ngắn gọn về chức năng.
    Mô tả chi tiết có thể kéo dài
   nhiều dòng nếu cần thiết.
    Args:
       param1: Mô tả param1
       param2: Mô tả param2
    Returns:
       Mô tả giá trị trả về
    Raises:
       ValueError: Trong trường hợp nào
       TypeError: Trong trường hợp nào
   """
   # Implementation
   pass
# Inline comment cách 2 khoảng trắng từ code
x = x + 1  # Increment x
```
#### 5. Khoảng trắng và Dòng trống
```python
# 5.1 Giữa functions cấp cao nhất: 2 dòng trống
def func1():
   pass

def func2():
   pass

# 5.2 Giữa methods trong class: 1 dòng trống
class MyClass:
   def method1(self):
       pass
   
   def method2(self):
       pass
# 5.3 Khoảng trắng trong expressions
# Tốt
x = 1
x = x + 1
list_comp = [x for x in range(10) if x % 2 == 0]
# Không tốt
x = 1
x = x + 1
list_comp=[x for x in range(10)if x%2==0]
```
#### 6. Type Hints và Annotations
```python
from typing import Dict, List, Optional, Union, Callable
# 6.1 Function annotations
def greet(name: str, times: int = 1) -> str:
   return "Hello " * times + name
# 6.2 Variable annotations
count: int = 0
names: List[str] = []
ser_map: Dict[int, str] = {}
# 6.3 Complex types
optionalInt = Optional[int]  # Same as Union[int, None]
rocessor = Callable[[str], bool]
def process_data(
   data: Union[str, bytes],
   callback: Optional[Processor] = None
) -> bool:
   pass
```
#### 7. Context Managers và Resource Handling
```python
# 7.1 File handling
# Tốt
with open('file.txt', 'r') as f:
   content = f.read()
# Không tốt
f = open('file.txt', 'r')
content = f.read()
f.close()
# 7.2 Custom context managers
from contextlib import contextmanager
@contextmanager
def resource_manager():
   try:
       # Setup
       yield
   finally:
       # Cleanup
       pass
```
#### 8. Exception Handling
```python
# 8.1 Specific exceptions
try:
   value = int(user_input)
except ValueError:
   handle_value_error()
except TypeError:
   handle_type_error()
else:
   handle_success()
finally:
   cleanup()
# 8.2 Custom exceptions
class ValidationError(Exception):
   """Custom exception for validation errors."""
   pass
# 8.3 Exception chaining
try:
   process_data()
except Exception as e:
   raise RuntimeError("Processing failed") from e
```

## 2. Quy ước Đặt Tên Theo Lĩnh Vực

### 2.1 Machine Learning / AI

#### Quy tắc chung
1. **Tên Model và Layers**
   - Sử dụng PascalCase cho tên model classes
   - Thêm hậu tố rõ ràng: Model, Net, Network
   - Đặt tên layer theo chức năng cụ thể
   - Tránh viết tắt không phổ biến

2. **Tham số và Hyperparameters**
   - Sử dụng snake_case cho tên biến
   - Prefix cho loại tham số: 
     - n_* cho số lượng (n_layers, n_units)
     - lr_* cho learning rate
     - max_* cho giới hạn trên
     - min_* cho giới hạn dưới
   - Constants viết hoa toàn bộ

3. **Dataset và DataLoader**
   - Suffix _dataset cho dataset classes
   - Suffix _loader cho dataloader classes
   - Prefix train_, val_, test_ cho data splits
   - Chỉ rõ batch_size trong tên biến liên quan

4. **Metrics và Logging**
   - Prefix val_ cho validation metrics
   - Prefix test_ cho test metrics
   - Sử dụng underscore cho composite metrics
   - Format số thập phân nhất quán

5. **Checkpoints và Artifacts**
   - Đặt tên theo pattern: {model_name}_{timestamp}_{metric}
   - Include version trong tên file
   - Tổ chức theo hierarchy rõ ràng
   - Lưu metadata kèm theo

#### Python Examples
```python
# 1. Model Definitions
class CNNClassifier(nn.Module):
    """Convolutional Neural Network for classification."""
    def __init__(
        self,
        n_classes: int,
        n_channels: int = 3,
        n_layers: int = 4,
        hidden_dim: int = 64
    ):
        super().__init__()
        self.n_classes = n_classes
        self._build_layers(n_channels, n_layers, hidden_dim)

# 2. Hyperparameters
TRAINING_CONFIG = {
    'lr_initial': 1e-3,
    'lr_min': 1e-6,
    'max_epochs': 100,
    'batch_size': 32,
    'weight_decay': 1e-5,
    'early_stop_patience': 10
}

# 3. Dataset Handling
class ImageClassificationDataset(Dataset):
    def __init__(
        self,
        data_root: str,
        transform: Optional[Callable] = None,
        target_size: Tuple[int, int] = (224, 224)
    ):
        self.data_root = Path(data_root)
        self.transform = transform
        self.target_size = target_size
        self._load_data()

# 4. Training Loop
def train_epoch(
    model: nn.Module,
    train_loader: DataLoader,
    criterion: nn.Module,
    optimizer: Optimizer,
    device: str = 'cuda'
) -> Dict[str, float]:
    """Train model for one epoch.

    Returns:
        Dict containing metrics:
        - loss: float
        - accuracy: float
        - learning_rate: float
    """
    metrics = defaultdict(float)
    # ... training logic ...
    return metrics

# 5. Metrics Tracking
class MetricsTracker:
    def __init__(self):
        self.metrics = {
            'train_loss': [],
            'train_accuracy': [],
            'val_loss': [],
            'val_accuracy': [],
            'learning_rate': []
        }

    def update(self, split: str, metrics: Dict[str, float]):
        for name, value in metrics.items():
            self.metrics[f'{split}_{name}'].append(value)

# 6. Model Checkpointing
def save_checkpoint(
    model: nn.Module,
    optimizer: Optimizer,
    epoch: int,
    metrics: Dict[str, float],
    save_dir: str
) -> str:
    """Save model checkpoint with metadata."""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    accuracy = f"{metrics['val_accuracy']:.4f}"
    
    checkpoint_name = (
        f"{model.__class__.__name__}_"
        f"epoch{epoch}_"
        f"acc{accuracy}_"
        f"{timestamp}.pt"
    )
    
    checkpoint_path = Path(save_dir) / checkpoint_name
    
    torch.save({
        'model_state_dict': model.state_dict(),
        'optimizer_state_dict': optimizer.state_dict(),
        'epoch': epoch,
        'metrics': metrics,
        'timestamp': timestamp,
        'config': model.config if hasattr(model, 'config') else None
    }, checkpoint_path)
    
    return str(checkpoint_path)
```

#### Best Practices
1. **Code Organization**
   - Tách model architecture và training logic
   - Modular design cho data processing
   - Centralized configuration management
   - Clear separation of concerns

2. **Documentation**
   - Docstring cho mọi class và method
   - Chỉ rõ shape của tensors
   - Document các assumptions
   - Ghi rõ requirements và dependencies

3. **Reproducibility**
   - Set random seeds
   - Log đầy đủ configuration
   - Version control cho data và models
   - Document environment setup

4. **Performance**
   - Profile code trước optimization
   - Sử dụng vectorization khi có thể
   - Implement proper batching
   - Monitor memory usage

5. **Testing**
   - Unit tests cho components
   - Integration tests cho pipeline
   - Validation của input/output
   - Performance benchmarks

### 2.2 Xử lý Ảnh/Video

#### Quy tắc chung
1. **Tên và Constants**
   - Suffix _img, _image cho biến ảnh
   - Suffix _vid, _video cho biến video
   - Prefix cho loại xử lý: raw_, processed_, augmented_
   - Constants cho các thông số chuẩn:
     - SIZE_*, DIM_* cho kích thước
     - FORMAT_* cho định dạng
     - CODEC_* cho video codecs

2. **Input/Output**
   - Validate đầy đủ input parameters
   - Kiểm tra định dạng và kích thước
   - Xử lý các trường hợp ngoại l��
   - Chuẩn hóa output format

3. **Image Processing Operations**
   - Prefix transform_ cho các hàm biến đổi
   - Prefix filter_ cho các hàm lọc
   - Prefix detect_ cho các hàm phát hiện
   - Prefix extract_ cho các hàm trích xuất features

4. **Video Processing**
   - Prefix frame_ cho xử lý từng frame
   - Prefix stream_ cho xử lý luồng
   - Chỉ rõ FPS trong tên biến liên quan
   - Quản lý bộ nhớ cho video dài

5. **Performance và Memory**
   - Batch processing cho hiệu suất
   - Lazy loading cho video
   - Giải phóng bộ nhớ kịp thời
   - Sử dụng generators khi thích hợp

#### Python Examples
```python
# 1. Constants và Configurations
IMAGE_FORMATS = {'jpg', 'png', 'tiff', 'bmp'}
VIDEO_FORMATS = {'mp4', 'avi', 'mov', 'mkv'}

SIZE_THUMBNAIL = (128, 128)
SIZE_DISPLAY = (800, 600)
SIZE_FULL_HD = (1920, 1080)

VIDEO_CODEC_H264 = cv2.VideoWriter_fourcc(*'H264')
VIDEO_CODEC_XVID = cv2.VideoWriter_fourcc(*'XVID')

# 2. Image Processing Classes
class ImageProcessor:
    """Base class for image processing operations."""
    
    def __init__(
        self,
        target_size: Optional[Tuple[int, int]] = None,
        normalize: bool = True
    ):
        self.target_size = target_size
        self.normalize = normalize
    
    def transform_resize(
        self,
        image: np.ndarray,
        interpolation: int = cv2.INTER_LINEAR
    ) -> np.ndarray:
        """Resize image to target size."""
        if self.target_size is None:
            return image
        return cv2.resize(image, self.target_size, interpolation=interpolation)
    
    def filter_gaussian_blur(
        self,
        image: np.ndarray,
        kernel_size: Tuple[int, int] = (5, 5)
    ) -> np.ndarray:
        """Apply Gaussian blur to image."""
        return cv2.GaussianBlur(image, kernel_size, 0)

# 3. Video Processing
class VideoProcessor:
    """Base class for video processing operations."""
    
    def __init__(
        self,
        target_fps: Optional[float] = None,
        batch_size: int = 32
    ):
        self.target_fps = target_fps
        self.batch_size = batch_size
        self._frame_buffer = []
    
    def stream_frames(
        self,
        video_path: str
    ) -> Generator[np.ndarray, None, None]:
        """Stream video frames efficiently."""
        cap = cv2.VideoCapture(video_path)
        try:
            while cap.isOpened():
                ret, frame = cap.read()
                if not ret:
                    break
                yield frame
        finally:
            cap.release()
    
    def process_batch(
        self,
        frames: List[np.ndarray]
    ) -> List[np.ndarray]:
        """Process a batch of frames."""
        processed = []
        for frame in frames:
            processed.append(self._process_single_frame(frame))
        return processed

# 4. Utility Functions
def validate_image(
    image: np.ndarray,
    min_size: Tuple[int, int] = (32, 32),
    max_size: Tuple[int, int] = (4096, 4096)
) -> bool:
    """Validate image dimensions and content."""
    if image is None or image.size == 0:
        return False
    h, w = image.shape[:2]
    return (min_size[0] <= h <= max_size[0] and 
            min_size[1] <= w <= max_size[1])

def save_video_frames(
    video_path: str,
    output_dir: str,
    frame_interval: int = 1
) -> List[str]:
    """Extract and save video frames."""
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    saved_paths = []
    
    for i, frame in enumerate(VideoProcessor().stream_frames(video_path)):
        if i % frame_interval == 0:
            path = f"{output_dir}/frame_{i:06d}.jpg"
            cv2.imwrite(path, frame)
            saved_paths.append(path)
    
    return saved_paths
```

#### Best Practices
1. **Memory Management**
   - Sử dụng generators cho video streams
   - Batch processing cho large images
   - Giải phóng resources trong context managers
   - Kiểm soát memory leaks

2. **Error Handling**
   - Validate input images/videos
   - Xử lý corrupt files
   - Graceful degradation
   - Logging đầy đủ errors

3. **Performance Optimization**
   - Sử dụng numpy operations
   - Tận dụng GPU khi có thể
   - Parallel processing cho batches
   - Caching kết quả trung gian

4. **Testing**
   - Unit tests với test images
   - Benchmark performance
   - Test edge cases
   - Validate output quality

5. **Documentation**
   - Input/output specifications
   - Performance characteristics
   - Memory requirements
   - Example usage

### 2.3 REST Services

#### Quy tắc chung
1. **URL và Routing**
   - Sử dụng kebab-case cho URLs
   - Version prefix cho APIs (v1, v2)
   - Resource names dạng số nhiều
   - Nested resources thể hiện quan hệ
   - Query params cho filtering/sorting

2. **HTTP Methods**
   - GET: Lấy resource
   - POST: Tạo resource mới
   - PUT: Update toàn bộ resource
   - PATCH: Update một phần resource
   - DELETE: Xóa resource

3. **Status Codes**
   - 2xx cho successful operations
   - 3xx cho redirections
   - 4xx cho client errors
   - 5xx cho server errors
   - Sử dụng đúng mục đích của từng code

4. **Request/Response Format**
   - Consistent content-type (JSON)
   - Chuẩn hóa response structure
   - Pagination cho list endpoints
   - Error response format thống nhất
   - Validate request data

5. **Security**
   - Authentication cho mọi endpoint
   - Rate limiting
   - Input sanitization
   - CORS configuration
   - Security headers

#### Python Examples
```python
# 1. Route Definitions
from fastapi import FastAPI, HTTPException, Security
from fastapi.security import APIKeyHeader

app = FastAPI(
    title="ML Model API",
    version="1.0.0",
    description="REST API for ML model predictions"
)

# 2. Authentication
api_key_header = APIKeyHeader(name="X-API-Key")

async def get_api_key(
    api_key_header: str = Security(api_key_header)
) -> str:
    if api_key_header == VALID_API_KEY:
        return api_key_header
    raise HTTPException(
        status_code=403,
        detail="Invalid API key"
    )

# 3. Request/Response Models
class PredictionRequest(BaseModel):
    image_url: str
    parameters: Optional[Dict[str, Any]] = None

    class Config:
        schema_extra = {
            "example": {
                "image_url": "https://example.com/image.jpg",
                "parameters": {"threshold": 0.5}
            }
        }

class PredictionResponse(BaseModel):
    prediction: List[str]
    confidence: List[float]
    processing_time: float

# 4. Endpoints
@app.post(
    "/api/v1/predict",
    response_model=PredictionResponse,
    tags=["Prediction"]
)
async def predict_image(
    request: PredictionRequest,
    api_key: str = Security(get_api_key)
) -> Dict[str, Any]:
    """Predict objects in image.
    
    Args:
        request: Image URL and optional parameters
        api_key: API key for authentication
    
    Returns:
        Predictions with confidence scores
    
    Raises:
        HTTPException: For invalid inputs or processing errors
    """
    try:
        start_time = time.perf_counter()
        predictions = await process_prediction(request)
        processing_time = time.perf_counter() - start_time
        
        return {
            "prediction": predictions.labels,
            "confidence": predictions.scores,
            "processing_time": processing_time
        }
    except ValueError as e:
        raise HTTPException(
            status_code=400,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Prediction error: {e}")
        raise HTTPException(
            status_code=500,
            detail="Internal processing error"
        )

# 5. Error Handling
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(
    request: Request,
    exc: RequestValidationError
) -> JSONResponse:
    return JSONResponse(
        status_code=422,
        content={
            "detail": "Validation error",
            "errors": exc.errors()
        }
    )

# 6. Middleware
@app.middleware("http")
async def add_security_headers(
    request: Request,
    call_next
) -> Response:
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    return response
```

#### Best Practices
1. **API Design**
   - RESTful principles
   - Consistent naming conventions
   - Clear documentation
   - Version control
   - Rate limiting

2. **Security**
   - Authentication/Authorization
   - Input validation
   - HTTPS only
   - Security headers
   - API key management

3. **Performance**
   - Caching strategies
   - Async operations
   - Database optimization
   - Response compression
   - Connection pooling

4. **Error Handling**
   - Detailed error messages
   - Consistent error format
   - Proper status codes
   - Error logging
   - Graceful degradation

5. **Documentation**
   - OpenAPI/Swagger docs
   - Example requests/responses
   - Authentication details
   - Rate limit info
   - Error scenarios

6. **Monitoring**
   - Request logging
   - Performance metrics
   - Error tracking
   - Usage analytics
   - Health checks

### 2.4 Data Engineering

#### Quy tắc chung

1. **Database Conventions**
   - Tên bảng: số nhiều, snake_case
   - Tên cột: snake_case, rõ nghĩa
   - Index: idx_{table}_{columns}
   - Foreign key: fk_{table}_{ref_table}
   - Timestamps: created_at, updated_at
   - Soft delete: deleted_at, is_active

2. **File Storage**
   - Phân cấp thư mục theo ngày: YYYY/MM/DD
   - Prefix theo loại: raw_, processed_, temp_
   - Versioning cho processed files
   - Metadata kèm theo mỗi file
   - Backup strategy rõ ràng

3. **Workflow Jobs**
   - Prefix theo loại: etl_, ml_, report_
   - Suffix theo tần suất: _daily, _hourly
   - Retry policy rõ ràng
   - Timeout settings phù hợp
   - Dependencies được ghi rõ

4. **Message Queue**
   - Topic naming: {domain}.{event_type}
   - Dead letter queues: dlq_{original_queue}
   - Priority levels rõ ràng
   - Message TTL được định nghĩa
   - Retry policies thống nhất

#### Python Examples

```python
# 1. Database Models (SQLAlchemy)
class ImageMetadata(Base):
    __tablename__ = 'image_metadata'
    
    id = Column(Integer, primary_key=True)
    file_path = Column(String(255), nullable=False)
    file_size = Column(Integer, nullable=False)
    dimensions = Column(JSON)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, onupdate=datetime.utcnow)
    deleted_at = Column(DateTime)
    
    __table_args__ = (
        Index('idx_image_metadata_path', 'file_path'),
        {'comment': 'Stores metadata for processed images'}
    )

# 2. File Storage Manager
class StorageManager:
    def __init__(self, base_path: str):
        self.base_path = Path(base_path)
    
    def generate_path(self, file_type: str) -> Path:
        """Generate hierarchical path: YYYY/MM/DD/file_type_*"""
        today = datetime.now()
        return self.base_path / str(today.year) / f"{today.month:02d}" / \
               f"{today.day:02d}" / f"{file_type}_{uuid.uuid4()}"
    
    def save_with_metadata(
        self,
        data: bytes,
        metadata: Dict[str, Any],
        file_type: str = 'raw'
    ) -> Tuple[Path, Dict]:
        path = self.generate_path(file_type)
        path.parent.mkdir(parents=True, exist_ok=True)
        
        # Save data
        path.write_bytes(data)
        
        # Save metadata
        meta_path = path.with_suffix('.meta.json')
        with meta_path.open('w') as f:
            json.dump({
                'file_path': str(path),
                'file_size': len(data),
                'created_at': datetime.now().isoformat(),
                **metadata
            }, f)
        
        return path, metadata

# 3. Workflow Definitions (Airflow)
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'data_team',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1)
}

dag = DAG(
    'etl_image_processing',
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False,
    tags=['etl', 'image']
)

# 4. Message Queue Handler
class MessageProcessor:
    def __init__(self, broker_url: str):
        self.connection = self.connect(broker_url)
        self.retry_policy = {
            'max_retries': 3,
            'initial_delay': 1,
            'max_delay': 300,
            'backoff_factor': 2
        }
    
    def publish(
        self,
        topic: str,
        message: Dict[str, Any],
        priority: int = 0
    ) -> None:
        """Publish message with metadata and tracking."""
        enriched_message = {
            'payload': message,
            'metadata': {
                'timestamp': datetime.utcnow().isoformat(),
                'message_id': str(uuid.uuid4()),
                'priority': priority
            }
        }
        self.connection.publish(topic, json.dumps(enriched_message))
    
    def consume(
        self,
        topic: str,
        handler: Callable,
        batch_size: int = 100
    ) -> None:
        """Consume messages with error handling and DLQ."""
        try:
            messages = self.connection.consume(topic, batch_size)
            for msg in messages:
                try:
                    handler(json.loads(msg.payload))
                    msg.ack()
                except Exception as e:
                    self._handle_failed_message(msg, e)
        except Exception as e:
            logger.error(f"Consumer error: {e}")
            raise

# 5. ETL Pipeline
class DataPipeline:
    def __init__(
        self,
        storage: StorageManager,
        processor: MessageProcessor,
        db_session: Session
    ):
        self.storage = storage
        self.processor = processor
        self.db_session = db_session
    
    def process_batch(
        self,
        batch_id: str,
        files: List[Path]
    ) -> None:
        """Process a batch of files with tracking."""
        try:
            # 1. Load and validate
            data = self._load_files(files)
            
            # 2. Transform
            processed = self._transform_data(data)
            
            # 3. Save results
            paths = self._save_results(processed, batch_id)
            
            # 4. Update database
            self._update_metadata(paths, batch_id)
            
            # 5. Notify completion
            self.processor.publish(
                'etl.batch.completed',
                {'batch_id': batch_id, 'status': 'success'}
            )
            
        except Exception as e:
            self.processor.publish(
                'etl.batch.failed',
                {'batch_id': batch_id, 'error': str(e)}
            )
            raise
```

#### Best Practices

1. **Database Management**
   - Connection pooling
   - Transaction management
   - Migration strategy
   - Indexing strategy
   - Partitioning plan

2. **Storage Optimization**
   - Compression policies
   - Retention policies
   - Access patterns
   - Backup strategy
   - Archival rules

3. **Workflow Management**
   - Dependency resolution
   - Resource allocation
   - Monitoring setup
   - Alert configuration
   - SLA definitions

4. **Queue Management**
   - Consumer groups
   - Partition strategy
   - Scaling policies
   - Error handling
   - Monitoring setup

5. **Performance**
   - Batch processing
   - Caching strategy
   - Resource limits
   - Timeout policies
   - Load balancing

6. **Monitoring**
   - Metrics collection
   - Log aggregation
   - Alert thresholds
   - Dashboard setup
   - Health checks

7. **Security**
   - Access control
   - Data encryption
   - Audit logging
   - Compliance checks
   - Vulnerability scanning

## 3. Tiêu Chuẩn Tài Liệu

### 3.1 Cấu Trúc Tài Liệu

#### 1. Tài Liệu Tổng Quan
```markdown
# Tên Dự Án

## 1. Giới Thiệu
- Mục đích
- Phạm vi
- Công nghệ sử dụng
- Yêu cầu hệ thống

## 2. Kiến Trúc Hệ Thống
- Sơ đồ kiến trúc tổng thể
- Các thành phần chính
- Luồng dữ liệu
- Giao tiếp giữa các services

## 3. Thiết Kế Chi Tiết
### 3.1 Database Schema
- ERD diagram
- Mô tả các bảng
- Relationships
- Indexing strategy

### 3.2 API Specifications
- REST endpoints
- Request/Response formats
- Authentication
- Rate limiting

### 3.3 Processing Pipelines
- Data flow diagrams
- Processing steps
- Error handling
- Performance considerations
```

#### 2. Tài Liệu Kỹ Thuật
```markdown
# Technical Documentation

## 1. Setup & Deployment
- Environment setup
- Dependencies
- Configuration
- Deployment process

## 2. Code Structure
- Project layout
- Key components
- Design patterns
- Coding conventions

## 3. Database
### 3.1 Schema Details
```sql
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### 3.2 Migrations
- Version control
- Rollback procedures
- Data seeding
```

#### 3. API Documentation
```python
@app.post("/api/v1/predict")
async def predict_image(
    request: PredictionRequest
) -> PredictionResponse:
    """Process image prediction request.
    
    Request Format:
    ```json
    {
        "image_url": "https://example.com/image.jpg",
        "parameters": {
            "threshold": 0.5,
            "max_detections": 10
        }
    }
    ```
    
    Response Format:
    ```json
    {
        "predictions": [
            {
                "label": "cat",
                "confidence": 0.95,
                "bbox": [x, y, w, h]
            }
        ],
        "processing_time": 0.45
    }
    ```
    
    Error Responses:
    - 400: Invalid input
    - 401: Unauthorized
    - 500: Processing error
    """
    pass
```

### 3.2 Docstrings

#### 1. Module Level
```python
"""
Image Processing Module

This module provides utilities for image processing and analysis.
It includes functions for loading, preprocessing, and analyzing images.

Key Components:
    - ImageLoader: Handles image loading and validation
    - ImageProcessor: Processes images with various algorithms
    - ImageAnalyzer: Analyzes processed images

Dependencies:
    - OpenCV
    - NumPy
    - TensorFlow

Example:
    >>> processor = ImageProcessor()
    >>> result = processor.process_image("image.jpg")
"""
```

#### 2. Class Level
```python
class ModelTrainer:
    """Neural network model trainer with customizable configurations.
    
    Attributes:
        model: Neural network model instance
        optimizer: Optimizer instance
        loss_fn: Loss function
        metrics: List of metrics to track
    
    Configuration:
        The trainer can be configured via a config dict with the following keys:
        - learning_rate: float
        - batch_size: int
        - num_epochs: int
        - early_stopping: bool
    
    Example:
        >>> trainer = ModelTrainer(model, optimizer)
        >>> history = trainer.train(train_data, valid_data)
    """
```

#### 3. Function Level
```python
def process_batch(
    images: List[np.ndarray],
    batch_size: int = 32,
    preprocessing_fn: Optional[Callable] = None
) -> Tuple[np.ndarray, Dict[str, Any]]:
    """Process a batch of images with optional preprocessing.
    
    Detailed Description:
        Processes multiple images in batches to optimize memory usage
        and processing speed. Can apply custom preprocessing functions.
    
    Args:
        images: List of images as numpy arrays
        batch_size: Number of images to process at once
        preprocessing_fn: Optional function to preprocess each image
    
    Returns:
        Tuple containing:
        - Processed images as numpy array
        - Metadata dictionary with processing info
    
    Raises:
        ValueError: If images are invalid or incompatible
        MemoryError: If batch size is too large
    
    Performance:
        - Time Complexity: O(n) where n is number of images
        - Space Complexity: O(batch_size)
    
    Notes:
        - Images must be same size
        - Preprocessing function must maintain image dimensions
    """
```

### 3.3 Comments

#### 1. Code Section Comments
```python
# 1. Data Loading and Validation
# =============================
def load_data():
    # Load raw data
    raw_data = pd.read_csv(DATA_PATH)
    
    # Validate schema
    _validate_schema(raw_data)
    
    # Clean missing values
    clean_data = _clean_missing_values(raw_data)
    
    return clean_data

# 2. Feature Engineering
# =====================
def engineer_features(data: pd.DataFrame) -> pd.DataFrame:
    """Feature engineering pipeline."""
    # Numeric features
    data = _process_numeric_features(data)
    
    # Categorical features
    data = _process_categorical_features(data)
    
    return data
```

#### 2. Complex Algorithm Comments
```python
def complex_algorithm(data: np.ndarray) -> np.ndarray:
    # 1. Initialize variables
    result = np.zeros_like(data)
    
    # 2. First pass: calculate preliminary values
    # This step is O(n) and requires temporary storage
    temp = np.mean(data, axis=0)
    
    # 3. Second pass: apply transformation
    # Using sliding window of size 3 for smoothing
    for i in range(1, len(data) - 1):
        result[i] = (data[i-1] + data[i] + data[i+1]) / 3
    
    return result
```