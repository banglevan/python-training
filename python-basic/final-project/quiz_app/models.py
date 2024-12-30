"""
Quiz application models
"""

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import List, Dict, Optional

class QuestionType(Enum):
    """Question type."""
    SINGLE_CHOICE = "single_choice"
    MULTIPLE_CHOICE = "multiple_choice"
    TRUE_FALSE = "true_false"

@dataclass
class Question:
    """Question model."""
    id: int
    text: str
    type: QuestionType
    choices: List[str]
    correct_answers: List[int]  # Indices of correct choices
    category: str
    points: int = 1
    explanation: Optional[str] = None

@dataclass
class QuizAttempt:
    """Quiz attempt model."""
    id: int
    user_id: int
    questions: List[Question]
    answers: Dict[int, List[int]]  # question_id -> selected choices
    started_at: datetime
    completed_at: Optional[datetime] = None
    
    def submit_answer(
        self,
        question_id: int,
        selected_choices: List[int]
    ):
        """Submit answer."""
        self.answers[question_id] = selected_choices
    
    def complete(self):
        """Complete attempt."""
        self.completed_at = datetime.now()
    
    def calculate_score(self) -> int:
        """Calculate score."""
        if not self.completed_at:
            return 0
            
        total = 0
        for question in self.questions:
            if question.id in self.answers:
                selected = set(self.answers[question.id])
                correct = set(question.correct_answers)
                if selected == correct:
                    total += question.points
        return total
    
    def get_results(self) -> Dict:
        """Get detailed results."""
        if not self.completed_at:
            return {}
            
        results = {
            "total_questions": len(self.questions),
            "answered_questions": len(self.answers),
            "correct_answers": 0,
            "total_points": 0,
            "score": 0,
            "duration": (
                self.completed_at - self.started_at
            ).total_seconds()
        }
        
        for question in self.questions:
            if question.id in self.answers:
                selected = set(self.answers[question.id])
                correct = set(question.correct_answers)
                if selected == correct:
                    results["correct_answers"] += 1
                    results["total_points"] += question.points
                    
        if results["total_questions"] > 0:
            results["score"] = (
                results["correct_answers"] /
                results["total_questions"] * 100
            )
            
        return results 