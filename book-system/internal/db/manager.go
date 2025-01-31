package db

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/mattn/go-sqlite3"
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
	_, err := m.db.Exec(`
        CREATE TABLE IF NOT EXISTS books (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            author TEXT NOT NULL,
            isbn TEXT,
            published_year INTEGER,
            publisher TEXT,
            description TEXT,
            category TEXT,
            stock INTEGER DEFAULT 0,
            created_by INTEGER,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            image_path TEXT,
            content_path TEXT,
            language TEXT,
            rights TEXT,
            source_url TEXT,
            source_id TEXT,
            format TEXT,
            file_size INTEGER
        )
    `)
	return err
}
