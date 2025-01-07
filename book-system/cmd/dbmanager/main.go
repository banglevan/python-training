package main

import (
    "bufio"
    "fmt"
    "os"
    "strings"
    "time"
    
    "booksystem/internal/db"
)

func main() {
    // Initialize database manager
    manager, err := db.NewManager("books.db")
    if err != nil {
        fmt.Printf("Error initializing database: %v\n", err)
        return
    }
    defer manager.Close()

    fmt.Println("Book System Database Manager")
    fmt.Println("Type 'help' for available commands")

    reader := bufio.NewReader(os.Stdin)
    for {
        fmt.Print("\nDB > ")
        input, _ := reader.ReadString('\n')
        input = strings.TrimSpace(input)

        if input == "exit" || input == "quit" {
            break
        }

        handleCommand(manager, input)
    }
}

func handleCommand(manager *db.DBManager, input string) {
    parts := strings.Fields(input)
    if len(parts) == 0 {
        return
    }

    command := strings.ToLower(parts[0])
    args := parts[1:]

    switch command {
    case "tables":
        listTables(manager)
    case "describe":
        if len(args) > 0 {
            describeTable(manager, args[0])
        } else {
            fmt.Println("Usage: describe <table_name>")
        }
    case "query":
        if len(args) > 0 {
            executeQuery(manager, strings.Join(args, " "))
        } else {
            fmt.Println("Usage: query <sql_query>")
        }
    case "backup":
        if len(args) > 0 {
            backupDatabase(manager, args[0])
        } else {
            filename := fmt.Sprintf("backup_%s.db", 
                time.Now().Format("2006-01-02_15-04-05"))
            backupDatabase(manager, filename)
        }
    case "help":
        showHelp()
    default:
        fmt.Println("Unknown command. Type 'help' for available commands")
    }
}

func listTables(manager *db.DBManager) {
    tables, err := manager.ListTables()
    if err != nil {
        fmt.Printf("Error listing tables: %v\n", err)
        return
    }

    fmt.Println("\nDatabase Tables:")
    fmt.Println("Name\t\tColumns\tRecords")
    fmt.Println(strings.Repeat("-", 40))
    for _, table := range tables {
        fmt.Printf("%s\t\t%d\t%d\n", 
            table.Name, len(table.Columns), table.Records)
    }
}

func describeTable(manager *db.DBManager, tableName string) {
    columns, err := manager.DescribeTable(tableName)
    if err != nil {
        fmt.Printf("Error describing table: %v\n", err)
        return
    }

    fmt.Printf("\nTable structure for '%s':\n", tableName)
    fmt.Println("Column\t\tType\t\tPK\tNot Null")
    fmt.Println(strings.Repeat("-", 50))
    for _, col := range columns {
        fmt.Printf("%s\t\t%s\t\t%v\t%v\n",
            col.Name, col.Type, col.IsPK, col.NotNull)
    }
}

func executeQuery(manager *db.DBManager, query string) {
    rows, err := manager.ExecuteQuery(query)
    if err != nil {
        fmt.Printf("Error executing query: %v\n", err)
        return
    }
    defer rows.Close()

    // Get column names
    columns, err := rows.Columns()
    if err != nil {
        fmt.Printf("Error getting columns: %v\n", err)
        return
    }

    // Print results
    fmt.Println("\nResults:")
    fmt.Println(strings.Join(columns, "\t"))
    fmt.Println(strings.Repeat("-", len(columns)*8))

    // Prepare value holders
    values := make([]interface{}, len(columns))
    valuePtrs := make([]interface{}, len(columns))
    for i := range columns {
        valuePtrs[i] = &values[i]
    }

    // Print rows
    for rows.Next() {
        err := rows.Scan(valuePtrs...)
        if err != nil {
            fmt.Printf("Error scanning row: %v\n", err)
            return
        }

        for _, val := range values {
            fmt.Printf("%v\t", val)
        }
        fmt.Println()
    }
}

func backupDatabase(manager *db.DBManager, filename string) {
    err := manager.Backup(filename)
    if err != nil {
        fmt.Printf("Error backing up database: %v\n", err)
        return
    }
    fmt.Printf("Database backed up to: %s\n", filename)
}

func showHelp() {
    fmt.Println("\nAvailable commands:")
    fmt.Println("tables              - List all tables")
    fmt.Println("describe <table>    - Show table structure")
    fmt.Println("query <sql>         - Execute SQL query")
    fmt.Println("backup [filename]   - Backup database")
    fmt.Println("help               - Show this help")
    fmt.Println("exit/quit          - Exit program")
}
