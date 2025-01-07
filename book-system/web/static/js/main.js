// API endpoints
const API = {
    auth: {
        login: '/api/login',
        logout: '/api/logout'
    },
    books: {
        list: '/api/books',
        get: (id) => `/api/books/${id}`,
        create: '/api/books',
        update: (id) => `/api/books/${id}`,
        search: '/api/books/search',
        updateStock: (id) => `/api/books/${id}/stock`
    },
    loans: {
        create: '/api/loans',
        return: (id) => `/api/loans/${id}/return`,
        overdue: '/api/loans/overdue'
    },
    reviews: {
        list: (bookId) => `/api/books/${bookId}/reviews`,
        create: '/api/reviews'
    },
    categories: {
        list: '/api/categories'
    },
    stats: {
        get: '/api/stats',
        popularBooks: '/api/stats/popular-books'
    }
};

// Authentication
async function handleLogin(event) {
    event.preventDefault();
    
    const username = document.getElementById('username').value;
    const password = document.getElementById('password').value;
    
    try {
        const response = await fetch('/api/login', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({ username, password })
        });

        const data = await response.json();
        
        if (response.ok) {
            window.location.href = '/books';
        } else {
            document.getElementById('error-message').textContent = data.message;
            document.getElementById('error-message').style.display = 'block';
        }
    } catch (error) {
        document.getElementById('error-message').textContent = 'An error occurred';
        document.getElementById('error-message').style.display = 'block';
    }
}

async function logout() {
    try {
        await fetch(API.auth.logout, { method: 'POST' });
        window.location.href = '/login';
    } catch (error) {
        showError('An error occurred during logout');
    }
}

// Book Management
async function searchBooks() {
    const query = document.getElementById('searchInput').value;
    try {
        const response = await fetch(`${API.books.search}?q=${encodeURIComponent(query)}`);
        const data = await response.json();
        if (response.ok) {
            updateBooksList(data.data);
        } else {
            showError(data.message);
        }
    } catch (error) {
        showError('Error searching books');
    }
}

async function handleBookSubmit(event) {
    event.preventDefault();
    const bookId = document.getElementById('bookId').value;
    const bookData = {
        title: document.getElementById('title').value,
        author: document.getElementById('author').value,
        isbn: document.getElementById('isbn').value,
        stock: parseInt(document.getElementById('stock').value)
    };

    try {
        const url = bookId ? API.books.update(bookId) : API.books.create;
        const method = bookId ? 'PUT' : 'POST';
        
        const response = await fetch(url, {
            method,
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(bookData)
        });

        const data = await response.json();
        if (response.ok) {
            closeModal();
            location.reload();
        } else {
            showError(data.message);
        }
    } catch (error) {
        showError('Error saving book');
    }
}

// Loan Management
async function borrowBook(bookId) {
    try {
        const response = await fetch(API.loans.create, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                book_id: bookId,
                due_date: new Date(Date.now() + 14 * 24 * 60 * 60 * 1000) // 14 days from now
            })
        });

        const data = await response.json();
        if (response.ok) {
            location.reload();
        } else {
            showError(data.message);
        }
    } catch (error) {
        showError('Error borrowing book');
    }
}

async function returnBook(loanId) {
    try {
        const response = await fetch(API.loans.return(loanId), {
            method: 'POST'
        });

        const data = await response.json();
        if (response.ok) {
            location.reload();
        } else {
            showError(data.message);
        }
    } catch (error) {
        showError('Error returning book');
    }
}

// UI Helpers
function showError(message) {
    const errorDiv = document.getElementById('error-message');
    if (errorDiv) {
        errorDiv.textContent = message;
        errorDiv.style.display = 'block';
    } else {
        alert(message);
    }
}

function closeModal() {
    const modal = document.getElementById('bookModal');
    if (modal) {
        modal.style.display = 'none';
    }
}

function showAddBookForm() {
    const modal = document.getElementById('bookModal');
    const form = document.getElementById('bookForm');
    const title = document.getElementById('modalTitle');
    
    if (modal && form && title) {
        form.reset();
        document.getElementById('bookId').value = '';
        title.textContent = 'Add New Book';
        modal.style.display = 'block';
    }
}

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    // Close modal when clicking outside
    window.onclick = function(event) {
        const modal = document.getElementById('bookModal');
        if (event.target === modal) {
            closeModal();
        }
    };
}); 