"""
Unit tests cho animal classification
Tiêu chí đánh giá:
1. Animal Classes (40%): Mammal, Bird, Fish, Reptile
2. Inheritance (20%): Kế thừa đúng
3. Polymorphism (20%): Đa hình đúng
4. Classifier (20%): AnimalClassifier hoạt động đúng
"""

import pytest
from animal import (
    Animal,
    Mammal,
    Bird,
    Fish,
    Reptile,
    AnimalClassifier,
    Species,
    Diet,
    Habitat
)

@pytest.fixture
def sample_species():
    """Fixture cho Species."""
    return {
        "tiger": Species(
            scientific_name="Panthera tigris",
            common_name="Tiger",
            conservation_status="EN",
            description="Large cat species"
        ),
        "eagle": Species(
            scientific_name="Aquila chrysaetos",
            common_name="Golden Eagle",
            conservation_status="LC",
            description="Large bird of prey"
        ),
        "shark": Species(
            scientific_name="Carcharodon carcharias",
            common_name="Great White Shark",
            conservation_status="VU",
            description="Large predatory fish"
        ),
        "snake": Species(
            scientific_name="Naja naja",
            common_name="Indian Cobra",
            conservation_status="LC",
            description="Venomous snake"
        )
    }

@pytest.fixture
def sample_animals(sample_species):
    """Fixture cho animals."""
    tiger = Mammal(
        species=sample_species["tiger"],
        diet=Diet.CARNIVORE,
        habitat=Habitat.LAND,
        fur_color="Orange with black stripes"
    )
    
    eagle = Bird(
        species=sample_species["eagle"],
        diet=Diet.CARNIVORE,
        wingspan=2.3,
        can_fly=True
    )
    
    shark = Fish(
        species=sample_species["shark"],
        diet=Diet.CARNIVORE,
        max_depth=1200,
        is_saltwater=True
    )
    
    snake = Reptile(
        species=sample_species["snake"],
        diet=Diet.CARNIVORE,
        habitat=Habitat.LAND,
        is_venomous=True
    )
    
    return {
        "tiger": tiger,
        "eagle": eagle,
        "shark": shark,
        "snake": snake
    }

class TestAnimalClasses:
    """
    Test animal classes (40%)
    Pass: Tính năng hoạt động đúng ≥ 95%
    Fail: Tính năng hoạt động đúng < 95%
    """
    
    def test_mammal(self, sample_animals):
        """Test Mammal class."""
        tiger = sample_animals["tiger"]
        
        # Properties
        assert tiger.species.common_name == "Tiger"
        assert tiger.diet == Diet.CARNIVORE
        assert tiger.habitat == Habitat.LAND
        assert tiger.fur_color == "Orange with black stripes"
        assert tiger.num_legs == 4
        
        # Methods
        assert isinstance(tiger.make_sound(), str)
        assert isinstance(tiger.move(), str)
        
        # String representation
        assert "Mammal" in str(tiger)
        assert "Tiger" in str(tiger)

    def test_bird(self, sample_animals):
        """Test Bird class."""
        eagle = sample_animals["eagle"]
        
        # Properties
        assert eagle.species.common_name == "Golden Eagle"
        assert eagle.diet == Diet.CARNIVORE
        assert eagle.habitat == Habitat.AIR
        assert eagle.wingspan == 2.3
        assert eagle.can_fly is True
        
        # Methods
        assert isinstance(eagle.make_sound(), str)
        assert eagle.move() == "Fly"
        
        # String representation
        assert "Bird" in str(eagle)
        assert "Golden Eagle" in str(eagle)

    def test_fish(self, sample_animals):
        """Test Fish class."""
        shark = sample_animals["shark"]
        
        # Properties
        assert shark.species.common_name == "Great White Shark"
        assert shark.diet == Diet.CARNIVORE
        assert shark.habitat == Habitat.WATER
        assert shark.max_depth == 1200
        assert shark.is_saltwater is True
        
        # Methods
        assert isinstance(shark.make_sound(), str)
        assert shark.move() == "Swim"
        
        # String representation
        assert "Fish" in str(shark)
        assert "Great White Shark" in str(shark)

    def test_reptile(self, sample_animals):
        """Test Reptile class."""
        snake = sample_animals["snake"]
        
        # Properties
        assert snake.species.common_name == "Indian Cobra"
        assert snake.diet == Diet.CARNIVORE
        assert snake.habitat == Habitat.LAND
        assert snake.is_venomous is True
        
        # Methods
        assert isinstance(snake.make_sound(), str)
        assert snake.move() == "Crawl"
        
        # String representation
        assert "Reptile" in str(snake)
        assert "Indian Cobra" in str(snake)

class TestInheritance:
    """
    Test inheritance (20%)
    Pass: Kế thừa đúng ≥ 95%
    Fail: Kế thừa đúng < 95%
    """
    
    def test_inheritance_chain(self):
        """Test inheritance chain."""
        # All inherit from Animal
        assert issubclass(Mammal, Animal)
        assert issubclass(Bird, Animal)
        assert issubclass(Fish, Animal)
        assert issubclass(Reptile, Animal)

    def test_abstract_methods(self):
        """Test abstract methods."""
        # Cannot instantiate Animal
        with pytest.raises(TypeError):
            Animal(
                species=Species("", "", ""),
                diet=Diet.CARNIVORE,
                habitat=Habitat.LAND
            )

class TestPolymorphism:
    """
    Test polymorphism (20%)
    Pass: Đa hình đúng ≥ 95%
    Fail: Đa hình đúng < 95%
    """
    
    def test_polymorphic_behavior(self, sample_animals):
        """Test polymorphic behavior."""
        animals = list(sample_animals.values())
        
        # All can make sound
        sounds = [animal.make_sound() for animal in animals]
        assert len(sounds) == 4
        assert all(isinstance(s, str) for s in sounds)
        
        # All can move
        moves = [animal.move() for animal in animals]
        assert len(moves) == 4
        assert all(isinstance(m, str) for m in moves)
        
        # All have string representation
        strings = [str(animal) for animal in animals]
        assert len(strings) == 4
        assert all(isinstance(s, str) for s in strings)

class TestAnimalClassifier:
    """
    Test classifier (20%)
    Pass: Classifier hoạt động đúng ≥ 95%
    Fail: Classifier hoạt động đúng < 95%
    """
    
    def test_classifier_operations(self, sample_animals):
        """Test classifier operations."""
        classifier = AnimalClassifier()
        
        # Add animals
        for animal in sample_animals.values():
            classifier.add_animal(animal)
        
        # Get by diet
        carnivores = classifier.get_by_diet(Diet.CARNIVORE)
        assert len(carnivores) == 4
        
        herbivores = classifier.get_by_diet(Diet.HERBIVORE)
        assert len(herbivores) == 0
        
        # Get by habitat
        land = classifier.get_by_habitat(Habitat.LAND)
        assert len(land) == 2
        
        water = classifier.get_by_habitat(Habitat.WATER)
        assert len(water) == 1
        
        air = classifier.get_by_habitat(Habitat.AIR)
        assert len(air) == 1
        
        # Get by class
        mammals = classifier.get_by_class(Mammal)
        assert len(mammals) == 1
        assert isinstance(mammals[0], Mammal)
        
        birds = classifier.get_by_class(Bird)
        assert len(birds) == 1
        assert isinstance(birds[0], Bird)
        
        # Get endangered
        endangered = classifier.get_endangered()
        assert len(endangered) == 2  # Tiger (EN) and Shark (VU)
        
        # Species count
        assert classifier.get_species_count() == 4
        
        # Habitat distribution
        dist = classifier.get_habitat_distribution()
        assert dist["land"] == 2
        assert dist["water"] == 1
        assert dist["air"] == 1
        assert dist["amphibian"] == 0 