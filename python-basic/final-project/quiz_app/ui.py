"""
Quiz application UI
"""

import tkinter as tk
from tkinter import ttk, messagebox
from typing import List, Optional

from .models import Question, QuizAttempt, QuestionType
from .storage import QuizStorage

class QuizUI:
    """Quiz UI."""
    
    def __init__(self, storage: QuizStorage):
        """Initialize UI."""
        self.storage = storage
        
        # Create main window
        self.root = tk.Tk()
        self.root.title("Quiz Application")
        self.root.geometry("800x600")
        
        # Create notebook for tabs
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(fill=tk.BOTH, expand=True)
        
        # Create tabs
        self._create_question_tab()
        self._create_quiz_tab()
        self._create_results_tab()
        
        # Current quiz attempt
        self.current_attempt: Optional[QuizAttempt] = None
        self.current_question_index = 0
    
    def run(self):
        """Run UI."""
        self.root.mainloop()
    
    def _create_question_tab(self):
        """Create question management tab."""
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="Questions")
        
        # Split into list and form
        list_frame = ttk.Frame(tab)
        list_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        form_frame = ttk.Frame(tab)
        form_frame.pack(side=tk.RIGHT, fill=tk.BOTH)
        
        # Question list
        columns = ("id", "category", "type", "text")
        self.question_tree = ttk.Treeview(
            list_frame,
            columns=columns,
            show="headings"
        )
        
        self.question_tree.heading("id", text="ID")
        self.question_tree.heading("category", text="Category")
        self.question_tree.heading("type", text="Type")
        self.question_tree.heading("text", text="Question")
        
        scrollbar = ttk.Scrollbar(
            list_frame,
            orient=tk.VERTICAL,
            command=self.question_tree.yview
        )
        
        self.question_tree.configure(
            yscrollcommand=scrollbar.set
        )
        
        self.question_tree.pack(
            side=tk.LEFT,
            fill=tk.BOTH,
            expand=True
        )
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        # Question form
        ttk.Label(form_frame, text="Question:").pack()
        self.question_text = tk.Text(form_frame, height=3)
        self.question_text.pack(fill=tk.X)
        
        ttk.Label(form_frame, text="Type:").pack()
        self.type_var = tk.StringVar()
        ttk.Combobox(
            form_frame,
            textvariable=self.type_var,
            values=[t.value for t in QuestionType]
        ).pack(fill=tk.X)
        
        ttk.Label(form_frame, text="Category:").pack()
        self.category_var = tk.StringVar()
        self.category_entry = ttk.Entry(
            form_frame,
            textvariable=self.category_var
        )
        self.category_entry.pack(fill=tk.X)
        
        ttk.Label(form_frame, text="Choices:").pack()
        self.choices_text = tk.Text(form_frame, height=5)
        self.choices_text.pack(fill=tk.X)
        
        ttk.Label(
            form_frame,
            text="Correct Answers (comma-separated):"
        ).pack()
        self.correct_var = tk.StringVar()
        ttk.Entry(
            form_frame,
            textvariable=self.correct_var
        ).pack(fill=tk.X)
        
        ttk.Label(form_frame, text="Points:").pack()
        self.points_var = tk.StringVar(value="1")
        ttk.Entry(
            form_frame,
            textvariable=self.points_var
        ).pack(fill=tk.X)
        
        ttk.Label(form_frame, text="Explanation:").pack()
        self.explanation_text = tk.Text(form_frame, height=3)
        self.explanation_text.pack(fill=tk.X)
        
        # Buttons
        button_frame = ttk.Frame(form_frame)
        button_frame.pack(fill=tk.X, pady=10)
        
        ttk.Button(
            button_frame,
            text="Add",
            command=self._add_question
        ).pack(side=tk.LEFT)
        
        ttk.Button(
            button_frame,
            text="Update",
            command=self._update_question
        ).pack(side=tk.LEFT)
        
        ttk.Button(
            button_frame,
            text="Delete",
            command=self._delete_question
        ).pack(side=tk.LEFT)
        
        # Bind selection
        self.question_tree.bind(
            "<Double-1>",
            self._on_question_selected
        )
        
        # Load questions
        self._load_questions()
    
    def _create_quiz_tab(self):
        """Create quiz taking tab."""
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="Take Quiz")
        
        # Quiz setup frame
        setup_frame = ttk.LabelFrame(
            tab,
            text="Quiz Setup"
        )
        setup_frame.pack(fill=tk.X, padx=5, pady=5)
        
        ttk.Label(
            setup_frame,
            text="Category:"
        ).pack(side=tk.LEFT)
        
        self.quiz_category_var = tk.StringVar()
        self.category_combo = ttk.Combobox(
            setup_frame,
            textvariable=self.quiz_category_var
        )
        self.category_combo.pack(side=tk.LEFT, padx=5)
        
        ttk.Label(
            setup_frame,
            text="User ID:"
        ).pack(side=tk.LEFT)
        
        self.user_id_var = tk.StringVar()
        ttk.Entry(
            setup_frame,
            textvariable=self.user_id_var,
            width=10
        ).pack(side=tk.LEFT, padx=5)
        
        ttk.Button(
            setup_frame,
            text="Start Quiz",
            command=self._start_quiz
        ).pack(side=tk.LEFT)
        
        # Quiz frame
        self.quiz_frame = ttk.LabelFrame(
            tab,
            text="Quiz"
        )
        self.quiz_frame.pack(
            fill=tk.BOTH,
            expand=True,
            padx=5,
            pady=5
        )
        
        # Update categories
        self._update_categories()
    
    def _create_results_tab(self):
        """Create results tab."""
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="Results")
        
        # Results list
        columns = (
            "id",
            "user_id",
            "questions",
            "score",
            "completed"
        )
        self.results_tree = ttk.Treeview(
            tab,
            columns=columns,
            show="headings"
        )
        
        self.results_tree.heading("id", text="ID")
        self.results_tree.heading("user_id", text="User")
        self.results_tree.heading(
            "questions",
            text="Questions"
        )
        self.results_tree.heading("score", text="Score")
        self.results_tree.heading(
            "completed",
            text="Completed"
        )
        
        scrollbar = ttk.Scrollbar(
            tab,
            orient=tk.VERTICAL,
            command=self.results_tree.yview
        )
        
        self.results_tree.configure(
            yscrollcommand=scrollbar.set
        )
        
        self.results_tree.pack(
            side=tk.LEFT,
            fill=tk.BOTH,
            expand=True
        )
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        # Bind double click
        self.results_tree.bind(
            "<Double-1>",
            self._show_attempt_details
        )
        
        # Load results
        self._load_results()
    
    def _add_question(self):
        """Add new question."""
        try:
            # Get form data
            question_data = {
                "text": self.question_text.get(
                    "1.0",
                    tk.END
                ).strip(),
                "type": self.type_var.get(),
                "category": self.category_var.get(),
                "choices": [
                    c.strip()
                    for c in self.choices_text.get(
                        "1.0",
                        tk.END
                    ).strip().split("\n")
                    if c.strip()
                ],
                "correct_answers": [
                    int(i.strip())
                    for i in self.correct_var.get().split(",")
                    if i.strip()
                ],
                "points": int(self.points_var.get()),
                "explanation": (
                    self.explanation_text.get(
                        "1.0",
                        tk.END
                    ).strip()
                )
            }
            
            # Validate
            if not question_data["text"]:
                raise ValueError("Question text required")
            if not question_data["choices"]:
                raise ValueError("Choices required")
            if not question_data["correct_answers"]:
                raise ValueError(
                    "Correct answers required"
                )
            
            # Add question
            self.storage.add_question(question_data)
            
            # Clear form and reload
            self._clear_question_form()
            self._load_questions()
            self._update_categories()
            
        except Exception as e:
            messagebox.showerror(
                "Error",
                f"Failed to add question: {str(e)}"
            )
    
    def _update_question(self):
        """Update selected question."""
        selection = self.question_tree.selection()
        if not selection:
            messagebox.showwarning(
                "Warning",
                "Please select a question to update"
            )
            return
            
        try:
            # Get question ID
            question_id = int(
                self.question_tree.item(
                    selection[0]
                )["values"][0]
            )
            
            # Get form data
            question_data = {
                "text": self.question_text.get(
                    "1.0",
                    tk.END
                ).strip(),
                "type": self.type_var.get(),
                "category": self.category_var.get(),
                "choices": [
                    c.strip()
                    for c in self.choices_text.get(
                        "1.0",
                        tk.END
                    ).strip().split("\n")
                    if c.strip()
                ],
                "correct_answers": [
                    int(i.strip())
                    for i in self.correct_var.get().split(",")
                    if i.strip()
                ],
                "points": int(self.points_var.get()),
                "explanation": (
                    self.explanation_text.get(
                        "1.0",
                        tk.END
                    ).strip()
                )
            }
            
            # Update question
            self.storage.update_question(
                question_id,
                question_data
            )
            
            # Clear form and reload
            self._clear_question_form()
            self._load_questions()
            
        except Exception as e:
            messagebox.showerror(
                "Error",
                f"Failed to update question: {str(e)}"
            )
    
    def _delete_question(self):
        """Delete selected question."""
        selection = self.question_tree.selection()
        if not selection:
            messagebox.showwarning(
                "Warning",
                "Please select a question to delete"
            )
            return
            
        if messagebox.askyesno(
            "Confirm",
            "Delete this question?"
        ):
            try:
                question_id = int(
                    self.question_tree.item(
                        selection[0]
                    )["values"][0]
                )
                self.storage.delete_question(question_id)
                self._clear_question_form()
                self._load_questions()
                self._update_categories()
                
            except Exception as e:
                messagebox.showerror(
                    "Error",
                    f"Failed to delete question: {str(e)}"
                )
    
    def _start_quiz(self):
        """Start new quiz attempt."""
        try:
            # Validate input
            category = self.quiz_category_var.get()
            if not category:
                raise ValueError("Please select category")
                
            user_id = self.user_id_var.get()
            if not user_id:
                raise ValueError("Please enter user ID")
                
            # Get questions
            questions = self.storage.get_questions_by_category(
                category
            )
            if not questions:
                raise ValueError(
                    f"No questions in category: {category}"
                )
            
            # Create attempt
            self.current_attempt = self.storage.create_attempt(
                int(user_id),
                questions
            )
            self.current_question_index = 0
            
            # Show first question
            self._show_current_question()
            
        except Exception as e:
            messagebox.showerror(
                "Error",
                f"Failed to start quiz: {str(e)}"
            )
    
    def _show_current_question(self):
        """Show current question."""
        # Clear quiz frame
        for widget in self.quiz_frame.winfo_children():
            widget.destroy()
        
        if not self.current_attempt:
            return
            
        # Get current question
        question = self.current_attempt.questions[
            self.current_question_index
        ]
        
        # Show question
        ttk.Label(
            self.quiz_frame,
            text=f"Question {self.current_question_index + 1}"
        ).pack()
        
        ttk.Label(
            self.quiz_frame,
            text=question.text,
            wraplength=400
        ).pack(pady=10)
        
        # Show choices
        self.choice_vars = []
        for i, choice in enumerate(question.choices):
            var = tk.BooleanVar()
            self.choice_vars.append(var)
            
            ttk.Checkbutton(
                self.quiz_frame,
                text=choice,
                variable=var
            ).pack()
        
        # Navigation buttons
        button_frame = ttk.Frame(self.quiz_frame)
        button_frame.pack(pady=10)
        
        if self.current_question_index > 0:
            ttk.Button(
                button_frame,
                text="Previous",
                command=self._prev_question
            ).pack(side=tk.LEFT)
        
        if (self.current_question_index <
            len(self.current_attempt.questions) - 1):
            ttk.Button(
                button_frame,
                text="Next",
                command=self._next_question
            ).pack(side=tk.LEFT)
        else:
            ttk.Button(
                button_frame,
                text="Finish",
                command=self._finish_quiz
            ).pack(side=tk.LEFT)
    
    def _prev_question(self):
        """Go to previous question."""
        self._save_current_answer()
        self.current_question_index -= 1
        self._show_current_question()
    
    def _next_question(self):
        """Go to next question."""
        self._save_current_answer()
        self.current_question_index += 1
        self._show_current_question()
    
    def _finish_quiz(self):
        """Finish quiz attempt."""
        self._save_current_answer()
        
        if messagebox.askyesno(
            "Confirm",
            "Finish this quiz?"
        ):
            self.storage.complete_attempt(
                self.current_attempt.id
            )
            self.current_attempt = None
            self._load_results()
            self.notebook.select(2)  # Switch to results tab
    
    def _save_current_answer(self):
        """Save current question answer."""
        if not self.current_attempt:
            return
            
        # Get selected choices
        selected = [
            i for i, var in enumerate(self.choice_vars)
            if var.get()
        ]
        
        # Save answer
        question = self.current_attempt.questions[
            self.current_question_index
        ]
        self.storage.submit_answer(
            self.current_attempt.id,
            question.id,
            selected
        )
    
    def _show_attempt_details(self, event):
        """Show attempt details."""
        selection = self.results_tree.selection()
        if not selection:
            return
            
        # Get attempt
        attempt_id = int(
            self.results_tree.item(selection[0])["values"][0]
        )
        attempt = self.storage.get_attempt(attempt_id)
        if not attempt:
            return
            
        # Create details window
        details = tk.Toplevel(self.root)
        details.title(f"Attempt {attempt_id} Details")
        details.geometry("600x400")
        
        # Show results
        results = attempt.get_results()
        
        ttk.Label(
            details,
            text=f"Score: {results['score']:.1f}%"
        ).pack()
        
        ttk.Label(
            details,
            text=f"Correct: {results['correct_answers']}/"
                 f"{results['total_questions']}"
        ).pack()
        
        ttk.Label(
            details,
            text=f"Points: {results['total_points']}"
        ).pack()
        
        ttk.Label(
            details,
            text=f"Duration: {results['duration']:.0f} seconds"
        ).pack()
        
        # Show questions and answers
        for i, question in enumerate(attempt.questions):
            frame = ttk.LabelFrame(
                details,
                text=f"Question {i+1}"
            )
            frame.pack(fill=tk.X, padx=5, pady=5)
            
            ttk.Label(
                frame,
                text=question.text,
                wraplength=500
            ).pack()
            
            # Show choices
            selected = set(
                attempt.answers.get(question.id, [])
            )
            correct = set(question.correct_answers)
            
            for j, choice in enumerate(question.choices):
                if j in selected:
                    if j in correct:
                        prefix = "✓"  # Correct
                    else:
                        prefix = "✗"  # Wrong
                else:
                    if j in correct:
                        prefix = "•"  # Missed
                    else:
                        prefix = " "  # Not selected
                        
                ttk.Label(
                    frame,
                    text=f"{prefix} {choice}"
                ).pack()
            
            if question.explanation:
                ttk.Label(
                    frame,
                    text=f"Explanation: {question.explanation}",
                    wraplength=500
                ).pack()
    
    def _load_questions(self):
        """Load questions into tree."""
        for item in self.question_tree.get_children():
            self.question_tree.delete(item)
            
        for question in self.storage.questions.values():
            self.question_tree.insert(
                "",
                tk.END,
                values=(
                    question.id,
                    question.category,
                    question.type.value,
                    question.text
                )
            )
    
    def _load_results(self):
        """Load results into tree."""
        for item in self.results_tree.get_children():
            self.results_tree.delete(item)
            
        for attempt in self.storage.attempts.values():
            if attempt.completed_at:
                results = attempt.get_results()
                self.results_tree.insert(
                    "",
                    tk.END,
                    values=(
                        attempt.id,
                        attempt.user_id,
                        results["total_questions"],
                        f"{results['score']:.1f}%",
                        attempt.completed_at.strftime(
                            "%Y-%m-%d %H:%M"
                        )
                    )
                )
    
    def _update_categories(self):
        """Update category list."""
        categories = sorted({
            q.category
            for q in self.storage.questions.values()
        })
        self.category_combo["values"] = categories 