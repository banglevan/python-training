package db

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/mattn/go-sqlite3"
	"golang.org/x/crypto/bcrypt"
)

type DBManager struct {
	db *sql.DB
}

type Column struct {
	Name    string
	Type    string
	IsPK    bool
	NotNull bool
}

type TableInfo struct {
	Name    string
	Columns []Column
	Records int
}

func NewManager(dbPath string) (*DBManager, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, err
	}

	manager := &DBManager{db: db}

	// Initialize database with tables and mock data
	if err := manager.initDatabase(); err != nil {
		db.Close()
		return nil, err
	}

	return manager, nil
}

func (m *DBManager) Initialize() error {
	// Create tables
	for _, query := range Schema {
		if _, err := m.db.Exec(query); err != nil {
			return fmt.Errorf("error creating table: %v", err)
		}
	}

	// Insert initial data
	for _, query := range InitialData {
		if _, err := m.db.Exec(query); err != nil {
			log.Printf("Warning: error inserting initial data: %v", err)
		}
	}

	return nil
}

func (m *DBManager) ListTables() ([]TableInfo, error) {
	rows, err := m.db.Query(`
        SELECT name FROM sqlite_master 
        WHERE type='table' 
        AND name NOT LIKE 'sqlite_%'
    `)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []TableInfo
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, err
		}

		// Get columns for each table
		columns, err := m.DescribeTable(tableName)
		if err != nil {
			return nil, err
		}

		// Get record count
		var count int
		err = m.db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&count)
		if err != nil {
			return nil, err
		}

		tables = append(tables, TableInfo{
			Name:    tableName,
			Columns: columns,
			Records: count,
		})
	}
	return tables, nil
}

func (m *DBManager) DescribeTable(tableName string) ([]Column, error) {
	rows, err := m.db.Query(`
        SELECT name, type, pk, notnull
        FROM pragma_table_info(?)
    `, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []Column
	for rows.Next() {
		var col Column
		if err := rows.Scan(&col.Name, &col.Type, &col.IsPK, &col.NotNull); err != nil {
			return nil, err
		}
		columns = append(columns, col)
	}
	return columns, nil
}

func (m *DBManager) ExecuteQuery(query string, args ...interface{}) (*sql.Rows, error) {
	return m.db.Query(query, args...)
}

func (m *DBManager) ExecuteUpdate(query string, args ...interface{}) (sql.Result, error) {
	return m.db.Exec(query, args...)
}

func (m *DBManager) Backup(filename string) error {
	// Implementation for database backup
	// This is a simplified version
	_, err := m.db.Exec(fmt.Sprintf("VACUUM INTO '%s'", filename))
	return err
}

func (m *DBManager) Close() error {
	return m.db.Close()
}

func (m *DBManager) DB() *sql.DB {
	return m.db
}

func (m *DBManager) initDatabase() error {
	// Create tables
	_, err := m.db.Exec(`
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL,
            email TEXT,
            role TEXT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    `)
	if err != nil {
		return err
	}

	// Check if admin user exists
	var count int
	err = m.db.QueryRow("SELECT COUNT(*) FROM users WHERE username = ?", "admin").Scan(&count)
	if err != nil {
		return err
	}

	// Create admin user if not exists
	if count == 0 {
		hashedPassword, err := bcrypt.GenerateFromPassword([]byte("admin123"), bcrypt.DefaultCost)
		if err != nil {
			return err
		}

		_, err = m.db.Exec(`
            INSERT INTO users (username, password, email, role)
            VALUES (?, ?, ?, ?)
        `, "admin", string(hashedPassword), "admin@example.com", "admin")

		if err != nil {
			return err
		}
		log.Printf("Admin user created successfully")
	}

	return nil
}
