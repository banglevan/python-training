"""
Quiz storage module
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from .models import Question, QuizAttempt, QuestionType

class QuizStorage:
    """Quiz storage."""
    
    def __init__(self, file_path: str):
        """Initialize storage."""
        self.file_path = Path(file_path)
        self.questions: Dict[int, Question] = {}
        self.attempts: Dict[int, QuizAttempt] = {}
        self._next_question_id = 1
        self._next_attempt_id = 1
        self._load()
    
    def add_question(self, question_data: dict) -> Question:
        """Add new question."""
        question = Question(
            id=self._next_question_id,
            text=question_data["text"],
            type=QuestionType(question_data["type"]),
            choices=question_data["choices"],
            correct_answers=question_data["correct_answers"],
            category=question_data["category"],
            points=question_data.get("points", 1),
            explanation=question_data.get("explanation")
        )
        
        self.questions[question.id] = question
        self._next_question_id += 1
        self._save()
        
        return question
    
    def get_question(self, question_id: int) -> Optional[Question]:
        """Get question by ID."""
        return self.questions.get(question_id)
    
    def update_question(
        self,
        question_id: int,
        question_data: dict
    ) -> Optional[Question]:
        """Update question."""
        question = self.get_question(question_id)
        if not question:
            return None
            
        # Update fields
        question.text = question_data.get(
            "text",
            question.text
        )
        question.type = QuestionType(
            question_data["type"]
        ) if "type" in question_data else question.type
        question.choices = question_data.get(
            "choices",
            question.choices
        )
        question.correct_answers = question_data.get(
            "correct_answers",
            question.correct_answers
        )
        question.category = question_data.get(
            "category",
            question.category
        )
        question.points = question_data.get(
            "points",
            question.points
        )
        question.explanation = question_data.get(
            "explanation",
            question.explanation
        )
        
        self._save()
        return question
    
    def delete_question(self, question_id: int) -> bool:
        """Delete question."""
        if question_id not in self.questions:
            return False
            
        del self.questions[question_id]
        self._save()
        return True
    
    def get_questions_by_category(
        self,
        category: str
    ) -> List[Question]:
        """Get questions by category."""
        return [
            q for q in self.questions.values()
            if q.category == category
        ]
    
    def create_attempt(
        self,
        user_id: int,
        questions: List[Question]
    ) -> QuizAttempt:
        """Create new quiz attempt."""
        attempt = QuizAttempt(
            id=self._next_attempt_id,
            user_id=user_id,
            questions=questions,
            answers={},
            started_at=datetime.now()
        )
        
        self.attempts[attempt.id] = attempt
        self._next_attempt_id += 1
        self._save()
        
        return attempt
    
    def get_attempt(
        self,
        attempt_id: int
    ) -> Optional[QuizAttempt]:
        """Get attempt by ID."""
        return self.attempts.get(attempt_id)
    
    def submit_answer(
        self,
        attempt_id: int,
        question_id: int,
        selected_choices: List[int]
    ) -> bool:
        """Submit answer for attempt."""
        attempt = self.get_attempt(attempt_id)
        if not attempt or attempt.completed_at:
            return False
            
        attempt.submit_answer(question_id, selected_choices)
        self._save()
        return True
    
    def complete_attempt(self, attempt_id: int) -> bool:
        """Complete quiz attempt."""
        attempt = self.get_attempt(attempt_id)
        if not attempt or attempt.completed_at:
            return False
            
        attempt.complete()
        self._save()
        return True
    
    def get_user_attempts(
        self,
        user_id: int,
        completed_only: bool = False
    ) -> List[QuizAttempt]:
        """Get attempts by user."""
        attempts = [
            a for a in self.attempts.values()
            if a.user_id == user_id
        ]
        
        if completed_only:
            attempts = [
                a for a in attempts
                if a.completed_at
            ]
            
        return attempts
    
    def _load(self):
        """Load from file."""
        if not self.file_path.exists():
            return
            
        with open(self.file_path, "r") as f:
            data = json.load(f)
            
        # Load questions
        self._next_question_id = data["next_question_id"]
        for q_data in data["questions"]:
            question = Question(
                id=q_data["id"],
                text=q_data["text"],
                type=QuestionType(q_data["type"]),
                choices=q_data["choices"],
                correct_answers=q_data["correct_answers"],
                category=q_data["category"],
                points=q_data.get("points", 1),
                explanation=q_data.get("explanation")
            )
            self.questions[question.id] = question
        
        # Load attempts
        self._next_attempt_id = data["next_attempt_id"]
        for a_data in data["attempts"]:
            questions = [
                self.questions[q_id]
                for q_id in a_data["question_ids"]
            ]
            
            attempt = QuizAttempt(
                id=a_data["id"],
                user_id=a_data["user_id"],
                questions=questions,
                answers=a_data["answers"],
                started_at=datetime.fromisoformat(
                    a_data["started_at"]
                ),
                completed_at=datetime.fromisoformat(
                    a_data["completed_at"]
                ) if a_data.get("completed_at") else None
            )
            self.attempts[attempt.id] = attempt
    
    def _save(self):
        """Save to file."""
        data = {
            "next_question_id": self._next_question_id,
            "next_attempt_id": self._next_attempt_id,
            "questions": [],
            "attempts": []
        }
        
        # Save questions
        for question in self.questions.values():
            q_data = {
                "id": question.id,
                "text": question.text,
                "type": question.type.value,
                "choices": question.choices,
                "correct_answers": question.correct_answers,
                "category": question.category,
                "points": question.points
            }
            if question.explanation:
                q_data["explanation"] = question.explanation
            data["questions"].append(q_data)
        
        # Save attempts
        for attempt in self.attempts.values():
            a_data = {
                "id": attempt.id,
                "user_id": attempt.user_id,
                "question_ids": [
                    q.id for q in attempt.questions
                ],
                "answers": attempt.answers,
                "started_at": attempt.started_at.isoformat()
            }
            if attempt.completed_at:
                a_data["completed_at"] = (
                    attempt.completed_at.isoformat()
                )
            data["attempts"].append(a_data)
        
        with open(self.file_path, "w") as f:
            json.dump(data, f, indent=2) 