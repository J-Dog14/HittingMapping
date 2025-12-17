"""
Analyze pitching_accel_data_v1.db for NULL values in max_ir related columns.
Generates a report on data completeness for max_ir metrics.
"""

import sqlite3
import pandas as pd
from pathlib import Path

# Configuration
DB_FILE = "pitching_accel_data_v1.db"

def analyze_max_ir_nulls():
    """Analyze NULL values in max_ir related columns."""
    
    # Check if database exists
    if not Path(DB_FILE).exists():
        print(f"Error: Database file '{DB_FILE}' not found.")
        print(f"Current directory: {Path.cwd()}")
        return
    
    print(f"Connecting to database: {DB_FILE}")
    conn = sqlite3.connect(DB_FILE)
    
    try:
        # Get table names
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        
        if not tables:
            print("No tables found in database.")
            return
        
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
        
        # Get all column names
        cursor.execute(f"PRAGMA table_info({main_table})")
        columns_info = cursor.fetchall()
        column_names = [col[1] for col in columns_info]
        
        # Find all max_ir related columns
        max_ir_columns = [col for col in column_names if 'max_ir' in col.lower() or 'maxir' in col.lower()]
        
        if not max_ir_columns:
            print("\nNo max_ir related columns found.")
            print("Available columns:")
            for col in column_names[:20]:  # Show first 20 columns
                print(f"  - {col}")
            if len(column_names) > 20:
                print(f"  ... and {len(column_names) - 20} more columns")
            return
        
        print(f"\nFound {len(max_ir_columns)} max_ir related column(s):")
        for col in max_ir_columns:
            print(f"  - {col}")
        
        # Get total row count
        cursor.execute(f"SELECT COUNT(*) FROM {main_table}")
        total_rows = cursor.fetchone()[0]
        print(f"\nTotal rows in table: {total_rows}")
        
        # Analyze NULL values for each max_ir column
        print("\n" + "="*70)
        print("NULL VALUE ANALYSIS FOR MAX_IR COLUMNS")
        print("="*70)
        
        results = []
        
        for col in max_ir_columns:
            # Count NULL values
            cursor.execute(f"SELECT COUNT(*) FROM {main_table} WHERE {col} IS NULL")
            null_count = cursor.fetchone()[0]
            
            # Count non-NULL values
            cursor.execute(f"SELECT COUNT(*) FROM {main_table} WHERE {col} IS NOT NULL")
            non_null_count = cursor.fetchone()[0]
            
            # Calculate percentage
            null_pct = (null_count / total_rows * 100) if total_rows > 0 else 0
            non_null_pct = (non_null_count / total_rows * 100) if total_rows > 0 else 0
            
            results.append({
                'Column': col,
                'Total Rows': total_rows,
                'NULL Count': null_count,
                'NULL %': f"{null_pct:.2f}%",
                'Non-NULL Count': non_null_count,
                'Non-NULL %': f"{non_null_pct:.2f}%"
            })
            
            print(f"\n{col}:")
            print(f"  NULL values: {null_count:,} ({null_pct:.2f}%)")
            print(f"  Non-NULL values: {non_null_count:,} ({non_null_pct:.2f}%)")
        
        # Create summary DataFrame
        df_results = pd.DataFrame(results)
        
        # Summary statistics
        print("\n" + "="*70)
        print("SUMMARY")
        print("="*70)
        
        total_null_rows = 0
        for col in max_ir_columns:
            cursor.execute(f"SELECT COUNT(*) FROM {main_table} WHERE {col} IS NULL")
            null_count = cursor.fetchone()[0]
            if null_count > 0:
                total_null_rows += 1
        
        print(f"\nColumns with NULL values: {total_null_rows} out of {len(max_ir_columns)}")
        
        # Find rows where ALL max_ir columns are NULL
        null_conditions = [f"{col} IS NULL" for col in max_ir_columns]
        all_null_query = f"SELECT COUNT(*) FROM {main_table} WHERE {' AND '.join(null_conditions)}"
        cursor.execute(all_null_query)
        all_null_count = cursor.fetchone()[0]
        all_null_pct = (all_null_count / total_rows * 100) if total_rows > 0 else 0
        
        print(f"\nRows where ALL max_ir columns are NULL: {all_null_count:,} ({all_null_pct:.2f}%)")
        
        # Find rows where ANY max_ir column is NULL
        any_null_query = f"SELECT COUNT(*) FROM {main_table} WHERE {' OR '.join(null_conditions)}"
        cursor.execute(any_null_query)
        any_null_count = cursor.fetchone()[0]
        any_null_pct = (any_null_count / total_rows * 100) if total_rows > 0 else 0
        
        print(f"Rows where ANY max_ir column is NULL: {any_null_count:,} ({any_null_pct:.2f}%)")
        
        # Find rows where ALL max_ir columns are NOT NULL
        all_not_null_query = f"SELECT COUNT(*) FROM {main_table} WHERE {' AND '.join([f'{col} IS NOT NULL' for col in max_ir_columns])}"
        cursor.execute(all_not_null_query)
        all_not_null_count = cursor.fetchone()[0]
        all_not_null_pct = (all_not_null_count / total_rows * 100) if total_rows > 0 else 0
        
        print(f"Rows where ALL max_ir columns are NOT NULL: {all_not_null_count:,} ({all_not_null_pct:.2f}%)")
        
        # Save results to CSV
        output_file = "max_ir_null_analysis.csv"
        df_results.to_csv(output_file, index=False)
        print(f"\nDetailed results saved to: {output_file}")
        
        # Show sample of rows with NULL max_ir values
        print("\n" + "="*70)
        print("SAMPLE ROWS WITH NULL MAX_IR VALUES")
        print("="*70)
        
        sample_query = f"""
        SELECT owner, variable, component, max_ir_velocity, max_ir_time, 
               time_max_ir_to_mer, time_max_ir_to_rel
        FROM {main_table}
        WHERE {' OR '.join(null_conditions)}
        LIMIT 10
        """
        
        try:
            df_sample = pd.read_sql_query(sample_query, conn)
            if not df_sample.empty:
                print("\nFirst 10 rows with NULL max_ir values:")
                print(df_sample.to_string(index=False))
            else:
                print("\nNo rows found with NULL max_ir values.")
        except Exception as e:
            print(f"\nCould not retrieve sample rows: {e}")
            print("(Some columns may not exist in the table)")
        
    except Exception as e:
        print(f"\nError analyzing database: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        conn.close()
        print("\n" + "="*70)
        print("Analysis complete.")
        print("="*70)

if __name__ == "__main__":
    analyze_max_ir_nulls()

