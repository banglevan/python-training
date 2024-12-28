"""
Database Design
- Schema design
- Normalization
- Indexing
"""

import psycopg2
from typing import List, Dict, Any
import logging
from dataclasses import dataclass

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class Column:
    """Database column definition."""
    name: str
    data_type: str
    nullable: bool = True
    primary_key: bool = False
    foreign_key: str = None
    unique: bool = False
    default: Any = None
    check: str = None

@dataclass
class Index:
    """Database index definition."""
    name: str
    columns: List[str]
    unique: bool = False
    method: str = 'btree'
    where: str = None
    include: List[str] = None

class DatabaseDesigner:
    """Database design handler."""
    
    def __init__(
        self,
        dbname: str,
        user: str,
        password: str,
        host: str = 'localhost',
        port: int = 5432
    ):
        """Initialize database connection."""
        self.conn_params = {
            'dbname': dbname,
            'user': user,
            'password': password,
            'host': host,
            'port': port
        }
        self.conn = None
        self.cur = None
    
    def connect(self):
        """Establish database connection."""
        try:
            self.conn = psycopg2.connect(**self.conn_params)
            self.cur = self.conn.cursor()
            logger.info("Database connection established")
        except Exception as e:
            logger.error(f"Connection error: {e}")
            raise
    
    def disconnect(self):
        """Close database connection."""
        try:
            if self.cur:
                self.cur.close()
            if self.conn:
                self.conn.close()
            logger.info("Database connection closed")
        except Exception as e:
            logger.error(f"Disconnection error: {e}")
            raise
    
    def create_table(
        self,
        table_name: str,
        columns: List[Column]
    ):
        """Create a new table."""
        try:
            # Build column definitions
            column_defs = []
            constraints = []
            
            for col in columns:
                # Basic column definition
                col_def = f"{col.name} {col.data_type}"
                
                if not col.nullable:
                    col_def += " NOT NULL"
                
                if col.default is not None:
                    col_def += f" DEFAULT {col.default}"
                
                column_defs.append(col_def)
                
                # Add constraints
                if col.primary_key:
                    constraints.append(
                        f"PRIMARY KEY ({col.name})"
                    )
                
                if col.foreign_key:
                    constraints.append(
                        f"FOREIGN KEY ({col.name}) "
                        f"REFERENCES {col.foreign_key}"
                    )
                
                if col.unique:
                    constraints.append(
                        f"UNIQUE ({col.name})"
                    )
                
                if col.check:
                    constraints.append(
                        f"CHECK ({col.check})"
                    )
            
            # Combine all definitions
            all_defs = column_defs + constraints
            create_query = f"""
                CREATE TABLE {table_name} (
                    {',\n'.join(all_defs)}
                )
            """
            
            self.cur.execute(create_query)
            self.conn.commit()
            
            logger.info(f"Created table: {table_name}")
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Table creation error: {e}")
            raise
    
    def create_index(
        self,
        table_name: str,
        index: Index
    ):
        """Create a new index."""
        try:
            # Build index definition
            create_query = (
                f"CREATE {'UNIQUE ' if index.unique else ''}"
                f"INDEX {index.name} "
                f"ON {table_name}"
            )
            
            # Add index method
            if index.method:
                create_query += f" USING {index.method}"
            
            # Add columns
            create_query += f" ({', '.join(index.columns)})"
            
            # Add included columns
            if index.include:
                create_query += (
                    f" INCLUDE ({', '.join(index.include)})"
                )
            
            # Add WHERE clause
            if index.where:
                create_query += f" WHERE {index.where}"
            
            self.cur.execute(create_query)
            self.conn.commit()
            
            logger.info(
                f"Created index {index.name} on {table_name}"
            )
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Index creation error: {e}")
            raise
    
    def analyze_table(
        self,
        table_name: str
    ) -> Dict:
        """Analyze table statistics."""
        try:
            # Get basic table stats
            self.cur.execute(f"""
                SELECT 
                    reltuples::bigint AS row_estimate,
                    pg_size_pretty(pg_total_relation_size(%s)) AS total_size,
                    pg_size_pretty(pg_table_size(%s)) AS table_size,
                    pg_size_pretty(pg_indexes_size(%s)) AS index_size
                FROM pg_class
                WHERE relname = %s
            """, [table_name, table_name, table_name, table_name])
            
            stats = dict(self.cur.fetchone())
            
            # Get column stats
            self.cur.execute(f"""
                SELECT 
                    column_name,
                    data_type,
                    is_nullable,
                    column_default
                FROM information_schema.columns
                WHERE table_name = %s
            """, [table_name])
            
            stats['columns'] = [
                dict(zip(
                    ['name', 'type', 'nullable', 'default'],
                    row
                ))
                for row in self.cur.fetchall()
            ]
            
            # Get index stats
            self.cur.execute(f"""
                SELECT
                    indexname,
                    indexdef
                FROM pg_indexes
                WHERE tablename = %s
            """, [table_name])
            
            stats['indexes'] = [
                dict(zip(['name', 'definition'], row))
                for row in self.cur.fetchall()
            ]
            
            return stats
            
        except Exception as e:
            logger.error(f"Table analysis error: {e}")
            raise
    
    def normalize_table(
        self,
        table_name: str,
        column_groups: Dict[str, List[str]]
    ):
        """Normalize table into separate tables."""
        try:
            for new_table, columns in column_groups.items():
                # Create new table
                column_list = ', '.join(columns)
                create_query = f"""
                    CREATE TABLE {new_table} AS
                    SELECT DISTINCT {column_list}
                    FROM {table_name}
                """
                self.cur.execute(create_query)
                
                # Add primary key
                self.cur.execute(f"""
                    ALTER TABLE {new_table}
                    ADD COLUMN id SERIAL PRIMARY KEY
                """)
                
                # Update original table
                self.cur.execute(f"""
                    ALTER TABLE {table_name}
                    ADD COLUMN {new_table}_id INTEGER
                """)
                
                # Set foreign key
                update_query = f"""
                    UPDATE {table_name} t
                    SET {new_table}_id = nt.id
                    FROM {new_table} nt
                    WHERE {' AND '.join(
                        f't.{col} = nt.{col}'
                        for col in columns
                    )}
                """
                self.cur.execute(update_query)
                
                # Add foreign key constraint
                self.cur.execute(f"""
                    ALTER TABLE {table_name}
                    ADD CONSTRAINT fk_{table_name}_{new_table}
                    FOREIGN KEY ({new_table}_id)
                    REFERENCES {new_table}(id)
                """)
                
                # Drop original columns
                for column in columns:
                    self.cur.execute(f"""
                        ALTER TABLE {table_name}
                        DROP COLUMN {column}
                    """)
            
            self.conn.commit()
            logger.info(f"Normalized table: {table_name}")
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Normalization error: {e}")
            raise

def main():
    """Main function."""
    # Initialize designer
    designer = DatabaseDesigner(
        dbname='test_db',
        user='test_user',
        password='test_pass'
    )
    
    try:
        # Connect to database
        designer.connect()
        
        # Example table creation
        columns = [
            Column(
                name='id',
                data_type='SERIAL',
                primary_key=True
            ),
            Column(
                name='name',
                data_type='VARCHAR(100)',
                nullable=False
            ),
            Column(
                name='email',
                data_type='VARCHAR(255)',
                unique=True,
                nullable=False
            ),
            Column(
                name='age',
                data_type='INTEGER',
                check='age >= 0'
            ),
            Column(
                name='created_at',
                data_type='TIMESTAMP',
                default='CURRENT_TIMESTAMP'
            )
        ]
        
        designer.create_table('users', columns)
        
        # Example index creation
        index = Index(
            name='idx_users_email',
            columns=['email'],
            unique=True
        )
        
        designer.create_index('users', index)
        
        # Example table analysis
        stats = designer.analyze_table('users')
        logger.info(f"Table statistics: {stats}")
        
        # Example normalization
        column_groups = {
            'user_details': ['age', 'address', 'phone'],
            'user_preferences': ['theme', 'language', 'timezone']
        }
        
        designer.normalize_table('users', column_groups)
        
    finally:
        # Close connection
        designer.disconnect()

if __name__ == '__main__':
    main() 