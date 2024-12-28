"""
Animal classification exercise
Minh họa inheritance và polymorphism với phân loại động vật
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import List, Optional, Set

class Diet(Enum):
    """Chế độ ăn."""
    HERBIVORE = "herbivore"  # Ăn thực vật
    CARNIVORE = "carnivore"  # Ăn thịt
    OMNIVORE = "omnivore"    # Ăn tạp

class Habitat(Enum):
    """Môi trường sống."""
    LAND = "land"        # Trên cạn
    WATER = "water"      # Dưới nước
    AMPHIBIAN = "amphibian"  # Lưỡng cư
    AIR = "air"          # Trên không

@dataclass
class Species:
    """Thông tin loài."""
    scientific_name: str     # Tên khoa học
    common_name: str         # Tên thường gọi
    conservation_status: str # Tình trạng bảo tồn
    description: Optional[str] = None

class Animal(ABC):
    """Abstract base class cho động vật."""
    
    def __init__(
        self,
        species: Species,
        diet: Diet,
        habitat: Habitat
    ):
        """
        Khởi tạo động vật.
        
        Args:
            species: Thông tin loài
            diet: Chế độ ăn
            habitat: Môi trường sống
        """
        self.species = species
        self.diet = diet
        self.habitat = habitat
    
    @abstractmethod
    def make_sound(self) -> str:
        """Tạo âm thanh."""
        pass
    
    @abstractmethod
    def move(self) -> str:
        """Di chuyển."""
        pass
    
    def __str__(self) -> str:
        """String representation."""
        return (
            f"{self.__class__.__name__}("
            f"name={self.species.common_name}, "
            f"diet={self.diet.value}, "
            f"habitat={self.habitat.value})"
        )

class Mammal(Animal):
    """Lớp động vật có vú."""
    
    def __init__(
        self,
        species: Species,
        diet: Diet,
        habitat: Habitat,
        fur_color: str,
        num_legs: int = 4
    ):
        """
        Khởi tạo động vật có vú.
        
        Args:
            fur_color: Màu lông
            num_legs: Số chân
        """
        super().__init__(species, diet, habitat)
        self.fur_color = fur_color
        self.num_legs = num_legs
    
    def make_sound(self) -> str:
        """Tạo âm thanh."""
        return "Some mammal sound"
    
    def move(self) -> str:
        """Di chuyển."""
        return "Walk/Run on legs"

class Bird(Animal):
    """Lớp chim."""
    
    def __init__(
        self,
        species: Species,
        diet: Diet,
        wingspan: float,
        can_fly: bool = True
    ):
        """
        Khởi tạo chim.
        
        Args:
            wingspan: Sải cánh
            can_fly: Có thể bay
        """
        super().__init__(species, diet, Habitat.AIR)
        self.wingspan = wingspan
        self.can_fly = can_fly
    
    def make_sound(self) -> str:
        """Tạo âm thanh."""
        return "Chirp/Tweet"
    
    def move(self) -> str:
        """Di chuyển."""
        return "Fly" if self.can_fly else "Walk"

class Fish(Animal):
    """Lớp cá."""
    
    def __init__(
        self,
        species: Species,
        diet: Diet,
        max_depth: float,
        is_saltwater: bool
    ):
        """
        Khởi tạo cá.
        
        Args:
            max_depth: Độ sâu tối đa
            is_saltwater: Sống ở nước mặn
        """
        super().__init__(species, diet, Habitat.WATER)
        self.max_depth = max_depth
        self.is_saltwater = is_saltwater
    
    def make_sound(self) -> str:
        """Tạo âm thanh."""
        return "Bubble sounds"
    
    def move(self) -> str:
        """Di chuyển."""
        return "Swim"

class Reptile(Animal):
    """Lớp bò sát."""
    
    def __init__(
        self,
        species: Species,
        diet: Diet,
        habitat: Habitat,
        is_venomous: bool
    ):
        """
        Khởi tạo bò sát.
        
        Args:
            is_venomous: Có nọc độc
        """
        super().__init__(species, diet, habitat)
        self.is_venomous = is_venomous
    
    def make_sound(self) -> str:
        """Tạo âm thanh."""
        return "Hiss"
    
    def move(self) -> str:
        """Di chuyển."""
        return "Crawl"

class AnimalClassifier:
    """Phân loại động vật."""
    
    def __init__(self):
        """Khởi tạo classifier."""
        self.animals: List[Animal] = []
    
    def add_animal(self, animal: Animal):
        """Thêm động vật."""
        self.animals.append(animal)
    
    def get_by_diet(self, diet: Diet) -> List[Animal]:
        """Lấy theo chế độ ăn."""
        return [
            animal for animal in self.animals
            if animal.diet == diet
        ]
    
    def get_by_habitat(self, habitat: Habitat) -> List[Animal]:
        """Lấy theo môi trường sống."""
        return [
            animal for animal in self.animals
            if animal.habitat == habitat
        ]
    
    def get_by_class(self, cls: type) -> List[Animal]:
        """Lấy theo lớp."""
        return [
            animal for animal in self.animals
            if isinstance(animal, cls)
        ]
    
    def get_endangered(self) -> List[Animal]:
        """Lấy các loài có nguy cơ tuyệt chủng."""
        endangered_statuses = {
            "CR", "EN", "VU"  # Critically Endangered, Endangered, Vulnerable
        }
        return [
            animal for animal in self.animals
            if animal.species.conservation_status in endangered_statuses
        ]
    
    def get_species_count(self) -> int:
        """Đếm số loài."""
        species = {
            animal.species.scientific_name
            for animal in self.animals
        }
        return len(species)
    
    def get_habitat_distribution(self) -> dict:
        """Thống kê phân bố theo môi trường."""
        distribution = {}
        for habitat in Habitat:
            count = len(self.get_by_habitat(habitat))
            distribution[habitat.value] = count
        return distribution
    
    def __str__(self) -> str:
        """String representation."""
        return "\n".join(str(animal) for animal in self.animals) 