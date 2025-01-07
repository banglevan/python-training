package db

// Schema definitions
var Schema = []string{
    `CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE NOT NULL,
        password TEXT NOT NULL,
        email TEXT UNIQUE NOT NULL,
        role TEXT NOT NULL DEFAULT 'user',
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )`,

    `CREATE TABLE IF NOT EXISTS books (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT NOT NULL,
        author TEXT NOT NULL,
        isbn TEXT UNIQUE,
        published_year INTEGER,
        publisher TEXT,
        description TEXT,
        category TEXT,
        stock INTEGER DEFAULT 0,
        created_by INTEGER,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(created_by) REFERENCES users(id)
    )`,

    `CREATE TABLE IF NOT EXISTS loans (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        book_id INTEGER,
        user_id INTEGER,
        loan_date DATETIME DEFAULT CURRENT_TIMESTAMP,
        due_date DATETIME NOT NULL,
        return_date DATETIME,
        status TEXT DEFAULT 'borrowed',
        FOREIGN KEY(book_id) REFERENCES books(id),
        FOREIGN KEY(user_id) REFERENCES users(id)
    )`,
}

// Initial admin user (password should be hashed in production)
var InitialData = []string{
    `INSERT OR IGNORE INTO users (username, password, email, role) 
     VALUES ('admin', '$2a$10$dxLxhXsGYmwrWIdL/zb.L.U7NUzrEJhQH.uWSYBGxHCHGvNxvYZHu', 
             'admin@example.com', 'admin')`,
}