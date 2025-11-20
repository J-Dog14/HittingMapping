"""
Clean pitching_accel_data_v1.db by removing rows with NULL values in max_ir related columns.
Can remove rows where ALL max_ir columns are NULL, or where ANY max_ir column is NULL.
"""

import sqlite3
import pandas as pd
from pathlib import Path
import shutil
from datetime import datetime
import sys

# Configuration
DB_FILE = "pitching_accel_data_v1.db"
BACKUP_DB = True  # Create backup before cleaning
REMOVE_MODE = "all_null"  # Options: "all_null" (remove if ALL are NULL), "any_null" (remove if ANY is NULL)

def create_backup(db_file):
    """Create a backup of the database file."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_file = f"{db_file}.backup_{timestamp}"
    shutil.copy2(db_file, backup_file)
    print(f"Backup created: {backup_file}")
    return backup_file

def find_max_ir_columns(conn, table_name):
    """Find all max_ir related columns in the table."""
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns_info = cursor.fetchall()
    column_names = [col[1] for col in columns_info]
    
    # Find all max_ir related columns
    max_ir_columns = [col for col in column_names if 'max_ir' in col.lower() or 'maxir' in col.lower()]
    return max_ir_columns

def analyze_before_cleaning(conn, table_name, max_ir_columns, remove_mode):
    """Analyze what will be removed before cleaning."""
    cursor = conn.cursor()
    
    # Get total row count
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    total_rows = cursor.fetchone()[0]
    
    # Build query based on remove mode
    if remove_mode == "all_null":
        null_conditions = [f"{col} IS NULL" for col in max_ir_columns]
        query = f"SELECT COUNT(*) FROM {table_name} WHERE {' AND '.join(null_conditions)}"
        description = "ALL max_ir columns are NULL"
    elif remove_mode == "any_null":
        null_conditions = [f"{col} IS NULL" for col in max_ir_columns]
        query = f"SELECT COUNT(*) FROM {table_name} WHERE {' OR '.join(null_conditions)}"
        description = "ANY max_ir column is NULL"
    else:
        raise ValueError(f"Unknown remove_mode: {remove_mode}")
    
    cursor.execute(query)
    rows_to_delete = cursor.fetchone()[0]
    rows_to_keep = total_rows - rows_to_delete
    
    return total_rows, rows_to_delete, rows_to_keep, description

def clean_database(dry_run=False):
    """Clean the database by removing rows with NULL max_ir values."""
    
    # Check if database exists
    if not Path(DB_FILE).exists():
        print(f"Error: Database file '{DB_FILE}' not found.")
        print(f"Current directory: {Path.cwd()}")
        return False
    
    print(f"Connecting to database: {DB_FILE}")
    conn = sqlite3.connect(DB_FILE)
    
    try:
        # Get table names
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        
        if not tables:
            print("No tables found in database.")
            return False
        
        print(f"\nFound {len(tables)} table(s): {[t[0] for t in tables]}")
        
        # Find the main table (likely 'pitches')
        main_table = None
        for table_name, in tables:
            if 'pitch' in table_name.lower():
                main_table = table_name
                break
        
        if not main_table:
            main_table = tables[0][0]
        
        print(f"\nAnalyzing table: {main_table}")
        
        # Find max_ir columns
        max_ir_columns = find_max_ir_columns(conn, main_table)
        
        if not max_ir_columns:
            print("\nNo max_ir related columns found.")
            print("Nothing to clean.")
            return False
        
        print(f"\nFound {len(max_ir_columns)} max_ir related column(s):")
        for col in max_ir_columns:
            print(f"  - {col}")
        
        # Analyze what will be removed
        print(f"\nRemove mode: {REMOVE_MODE}")
        total_rows, rows_to_delete, rows_to_keep, description = analyze_before_cleaning(
            conn, main_table, max_ir_columns, REMOVE_MODE
        )
        
        print("\n" + "="*70)
        print("CLEANING ANALYSIS")
        print("="*70)
        print(f"\nTotal rows: {total_rows:,}")
        print(f"Rows to DELETE (where {description}): {rows_to_delete:,} ({rows_to_delete/total_rows*100:.2f}%)")
        print(f"Rows to KEEP: {rows_to_keep:,} ({rows_to_keep/total_rows*100:.2f}%)")
        
        if rows_to_delete == 0:
            print("\nNo rows to delete. Database is already clean.")
            return True
        
        if dry_run:
            print("\n" + "="*70)
            print("DRY RUN MODE - No changes will be made")
            print("="*70)
            return True
        
        # Confirm deletion
        print("\n" + "="*70)
        response = input(f"\nAre you sure you want to delete {rows_to_delete:,} rows? (yes/no): ").strip().lower()
        
        if response not in ['yes', 'y']:
            print("Cleaning cancelled.")
            return False
        
        # Create backup if requested
        if BACKUP_DB:
            print("\nCreating backup...")
            backup_file = create_backup(DB_FILE)
            print(f"Backup saved to: {backup_file}")
        
        # Build delete query
        if REMOVE_MODE == "all_null":
            null_conditions = [f"{col} IS NULL" for col in max_ir_columns]
            delete_query = f"DELETE FROM {main_table} WHERE {' AND '.join(null_conditions)}"
        elif REMOVE_MODE == "any_null":
            null_conditions = [f"{col} IS NULL" for col in max_ir_columns]
            delete_query = f"DELETE FROM {main_table} WHERE {' OR '.join(null_conditions)}"
        else:
            raise ValueError(f"Unknown remove_mode: {REMOVE_MODE}")
        
        # Execute deletion
        print(f"\nExecuting deletion query...")
        print(f"Query: {delete_query}")
        cursor.execute(delete_query)
        rows_deleted = cursor.rowcount
        
        # Commit changes
        conn.commit()
        
        # Verify deletion
        cursor.execute(f"SELECT COUNT(*) FROM {main_table}")
        remaining_rows = cursor.fetchone()[0]
        
        print("\n" + "="*70)
        print("CLEANING COMPLETE")
        print("="*70)
        print(f"\nRows deleted: {rows_deleted:,}")
        print(f"Rows remaining: {remaining_rows:,}")
        print(f"Expected remaining: {rows_to_keep:,}")
        
        if remaining_rows == rows_to_keep:
            print("\n✓ Deletion successful - row counts match expected values")
        else:
            print(f"\n⚠ Warning - row counts don't match exactly (expected {rows_to_keep:,}, got {remaining_rows:,})")
        
        # Vacuum database to reclaim space
        print("\nVacuuming database to reclaim space...")
        conn.execute("VACUUM")
        print("✓ Database vacuumed")
        
        return True
        
    except Exception as e:
        print(f"\nError cleaning database: {e}")
        import traceback
        traceback.print_exc()
        conn.rollback()
        return False
    
    finally:
        conn.close()

def main():
    """Main function with command line argument support."""
    dry_run = False
    
    # Check for command line arguments
    if len(sys.argv) > 1:
        if '--dry-run' in sys.argv or '-d' in sys.argv:
            dry_run = True
        if '--help' in sys.argv or '-h' in sys.argv:
            print("""
Usage: python clean_pitching_db.py [options]

Options:
  --dry-run, -d    Show what would be deleted without making changes
  --help, -h       Show this help message

Configuration (edit script to change):
  DB_FILE          Database file to clean (default: pitching_accel_data_v1.db)
  BACKUP_DB        Create backup before cleaning (default: True)
  REMOVE_MODE      "all_null" = remove if ALL max_ir columns are NULL
                   "any_null" = remove if ANY max_ir column is NULL
            """)
            return
    
    if dry_run:
        print("="*70)
        print("DRY RUN MODE - No changes will be made")
        print("="*70)
    
    success = clean_database(dry_run=dry_run)
    
    if success:
        print("\n" + "="*70)
        print("Script completed successfully.")
        print("="*70)
    else:
        print("\n" + "="*70)
        print("Script completed with errors.")
        print("="*70)
        sys.exit(1)

if __name__ == "__main__":
    main()

