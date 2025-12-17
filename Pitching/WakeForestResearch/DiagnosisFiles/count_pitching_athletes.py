"""
Count the number of athlete folders in H:/Pitching/Data
and compare with database entries to diagnose matching issues.
"""

import os
from pathlib import Path
import sqlite3
import pandas as pd

# Configuration
DATA_ROOT = "H:/Pitching/Data"
DB_FILE = "pitching_accel_data_v1.db"

def count_athlete_folders():
    """Count athlete folders in the data directory."""
    
    data_path = Path(DATA_ROOT)
    
    if not data_path.exists():
        print(f"Error: Data directory '{DATA_ROOT}' not found.")
        return None
    
    print(f"Scanning directory: {DATA_ROOT}")
    
    # Count folders (athletes)
    athlete_folders = []
    for item in data_path.iterdir():
        if item.is_dir():
            athlete_folders.append(item.name)
    
    athlete_folders.sort()
    
    print(f"\nFound {len(athlete_folders)} athlete folders")
    
    return athlete_folders

def analyze_database_names():
    """Analyze names in the database."""
    
    if not Path(DB_FILE).exists():
        print(f"\nWarning: Database '{DB_FILE}' not found. Skipping database analysis.")
        return None
    
    print(f"\nAnalyzing database: {DB_FILE}")
    conn = sqlite3.connect(DB_FILE)
    
    try:
        # Get table names
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        
        if not tables:
            print("No tables found in database.")
            return None
        
        # Find the main table
        main_table = None
        for table_name, in tables:
            if 'pitch' in table_name.lower():
                main_table = table_name
                break
        
        if not main_table:
            main_table = tables[0][0]
        
        # Read unique names
        df = pd.read_sql_query(f"SELECT DISTINCT name, athlete_id FROM {main_table} WHERE name IS NOT NULL", conn)
        
        return df
    
    except Exception as e:
        print(f"Error reading database: {e}")
        return None
    
    finally:
        conn.close()

def check_session_files(athlete_folders):
    """Check for session.xml files in athlete folders."""
    
    data_path = Path(DATA_ROOT)
    session_files_found = []
    session_data_files_found = []
    
    print(f"\nChecking for session.xml and session_data.xml files...")
    
    for folder_name in athlete_folders[:20]:  # Check first 20 as sample
        folder_path = data_path / folder_name
        
        # Look for session.xml
        session_xml = list(folder_path.rglob("session.xml"))
        session_data_xml = list(folder_path.rglob("session_data.xml"))
        
        if session_xml:
            session_files_found.append(folder_name)
        if session_data_xml:
            session_data_files_found.append(folder_name)
    
    print(f"Sample check (first 20 folders):")
    print(f"  Folders with session.xml: {len(session_files_found)}")
    print(f"  Folders with session_data.xml: {len(session_data_files_found)}")
    
    return session_files_found, session_data_files_found

def main():
    """Main analysis function."""
    
    print("="*70)
    print("PITCHING DATA DIRECTORY ANALYSIS")
    print("="*70)
    
    # Count athlete folders
    athlete_folders = count_athlete_folders()
    
    if athlete_folders is None:
        return
    
    print(f"\nFirst 20 athlete folders:")
    for i, folder in enumerate(athlete_folders[:20], 1):
        print(f"  {i:3d}. {folder}")
    if len(athlete_folders) > 20:
        print(f"  ... and {len(athlete_folders) - 20} more")
    
    # Check for session files
    session_files, session_data_files = check_session_files(athlete_folders)
    
    # Analyze database
    db_names_df = analyze_database_names()
    
    if db_names_df is not None:
        unique_db_names = db_names_df['name'].nunique()
        print(f"\n" + "="*70)
        print("DATABASE vs DIRECTORY COMPARISON")
        print("="*70)
        print(f"\nAthlete folders in directory: {len(athlete_folders)}")
        print(f"Unique names in database: {unique_db_names}")
        print(f"\nDifference: {len(athlete_folders) - unique_db_names}")
        
        if len(athlete_folders) > unique_db_names:
            print(f"\n⚠ WARNING: Database has {len(athlete_folders) - unique_db_names} FEWER unique names than folders!")
            print(f"This suggests the matching logic is incorrectly grouping multiple athletes together.")
        
        print(f"\nUnique names in database:")
        for name in db_names_df['name'].unique():
            count = len(db_names_df[db_names_df['name'] == name])
            print(f"  - {name} ({count} entries)")
        
        # Check which folder names might match database names
        print(f"\n" + "="*70)
        print("FOLDER NAME vs DATABASE NAME COMPARISON")
        print("="*70)
        
        # Try to match folder names to database names
        matches_found = 0
        unmatched_folders = []
        
        for folder_name in athlete_folders[:50]:  # Check first 50
            # Try different matching strategies
            folder_lower = folder_name.lower()
            matched = False
            
            for db_name in db_names_df['name'].unique():
                db_name_lower = db_name.lower()
                # Check if folder name contains parts of database name or vice versa
                if (folder_lower in db_name_lower or 
                    db_name_lower in folder_lower or
                    any(part in db_name_lower for part in folder_lower.split() if len(part) > 3)):
                    matched = True
                    matches_found += 1
                    break
            
            if not matched:
                unmatched_folders.append(folder_name)
        
        print(f"\nPotential matches found: {matches_found} out of {min(50, len(athlete_folders))} folders checked")
        print(f"Unmatched folders (first 20):")
        for folder in unmatched_folders[:20]:
            print(f"  - {folder}")
    
    print(f"\n" + "="*70)
    print("SUMMARY")
    print("="*70)
    print(f"\nTotal athlete folders: {len(athlete_folders)}")
    if db_names_df is not None:
        print(f"Unique names in database: {db_names_df['name'].nunique()}")
        print(f"\n⚠ Expected: {len(athlete_folders)} unique names")
        print(f"⚠ Actual: {db_names_df['name'].nunique()} unique names")
        print(f"\nThis suggests the matching logic in the R script needs to be fixed.")
    
    print("\n" + "="*70)

if __name__ == "__main__":
    main()

