"""
Unit tests cho Quiz Application
Tiêu chí đánh giá:
1. Question Management (35%): Question CRUD
2. Quiz Taking (35%): Quiz workflow
3. Scoring System (30%): Score calculation
"""

import pytest
from datetime import datetime
from pathlib import Path

from quiz_app.models import (
    Question,
    QuizAttempt,
    QuestionType
)
from quiz_app.storage import QuizStorage

@pytest.fixture
def temp_file(tmp_path):
    """Fixture cho temporary file."""
    return tmp_path / "quiz.json"

@pytest.fixture
def storage(temp_file):
    """Fixture cho storage."""
    return QuizStorage(str(temp_file))

@pytest.fixture
def sample_question():
    """Fixture cho sample question."""
    return {
        "text": "What is Python?",
        "type": "single_choice",
        "choices": [
            "A programming language",
            "A snake",
            "A movie",
            "A book"
        ],
        "correct_answers": [0],
        "category": "Programming",
        "points": 2,
        "explanation": "Python is a programming language"
    }

class TestQuestionManagement:
    """Test question management (35%)."""
    
    def test_add_question(self, storage, sample_question):
        """Test adding question."""
        question = storage.add_question(sample_question)
        
        assert question.id == 1
        assert question.text == sample_question["text"]
        assert question.type == QuestionType.SINGLE_CHOICE
        assert len(question.choices) == 4
        assert question.points == 2
        
        # Verify persistence
        loaded = storage.get_question(1)
        assert loaded is not None
        assert loaded.text == question.text
    
    def test_update_question(self, storage, sample_question):
        """Test updating question."""
        # Add question
        question = storage.add_question(sample_question)
        
        # Update question
        updated = storage.update_question(
            question.id,
            {
                "text": "Updated question",
                "points": 3
            }
        )
        
        assert updated is not None
        assert updated.text == "Updated question"
        assert updated.points == 3
        
        # Verify persistence
        loaded = storage.get_question(question.id)
        assert loaded.text == "Updated question"
    
    def test_delete_question(self, storage, sample_question):
        """Test deleting question."""
        # Add question
        question = storage.add_question(sample_question)
        
        # Delete question
        assert storage.delete_question(question.id)
        assert storage.get_question(question.id) is None

class TestQuizTaking:
    """Test quiz taking (35%)."""
    
    def test_quiz_workflow(self, storage, sample_question):
        """Test quiz workflow."""
        # Add questions
        q1 = storage.add_question(sample_question)
        q2 = storage.add_question({
            "text": "Is Python interpreted?",
            "type": "true_false",
            "choices": ["True", "False"],
            "correct_answers": [0],
            "category": "Programming"
        })
        
        # Create attempt
        attempt = storage.create_attempt(
            user_id=1,
            questions=[q1, q2]
        )
        
        assert attempt.id == 1
        assert len(attempt.questions) == 2
        assert not attempt.completed_at
        
        # Submit answers
        storage.submit_answer(attempt.id, q1.id, [0])
        storage.submit_answer(attempt.id, q2.id, [0])
        
        # Complete attempt
        assert storage.complete_attempt(attempt.id)
        
        # Verify completion
        attempt = storage.get_attempt(attempt.id)
        assert attempt.completed_at is not None
        
        # Verify answers
        assert attempt.answers[q1.id] == [0]
        assert attempt.answers[q2.id] == [0]

class TestScoringSystem:
    """Test scoring system (30%)."""
    
    def test_score_calculation(
        self,
        storage,
        sample_question
    ):
        """Test score calculation."""
        # Add questions
        q1 = storage.add_question(sample_question)  # 2 points
        q2 = storage.add_question({
            "text": "Is Python interpreted?",
            "type": "true_false",
            "choices": ["True", "False"],
            "correct_answers": [0],
            "category": "Programming",
            "points": 1
        })  # 1 point
        
        # Create and complete attempt
        attempt = storage.create_attempt(
            user_id=1,
            questions=[q1, q2]
        )
        
        # All correct
        storage.submit_answer(attempt.id, q1.id, [0])
        storage.submit_answer(attempt.id, q2.id, [0])
        storage.complete_attempt(attempt.id)
        
        results = attempt.get_results()
        assert results["total_questions"] == 2
        assert results["correct_answers"] == 2
        assert results["total_points"] == 3
        assert results["score"] == 100
        
        # Partial correct
        attempt = storage.create_attempt(
            user_id=1,
            questions=[q1, q2]
        )
        storage.submit_answer(attempt.id, q1.id, [1])  # Wrong
        storage.submit_answer(attempt.id, q2.id, [0])  # Correct
        storage.complete_attempt(attempt.id)
        
        results = attempt.get_results()
        assert results["correct_answers"] == 1
        assert results["total_points"] == 1
        assert results["score"] == 50 