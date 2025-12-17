"""
Export anonymized acceleration data from pitching_accel_data_v1.db to CSV.
Replaces athlete names with Subject IDs (S001, S002, etc.) and removes identifying columns.
"""

import sqlite3
import pandas as pd
from pathlib import Path

# Configuration
DB_FILE = "pitching_accel_data_v1.db"
OUTPUT_CSV = "Accel Data.csv"
OUTPUT_CSV_16_PLUS = "Accel Data 16 Plus.csv"
OUTPUT_CSV_15_PLUS = "Accel Data 15 Plus.csv"
TABLE_NAME = "pitches"

# Columns to remove
COLUMNS_TO_REMOVE = ["source_path", "source_data_path", "source_data_file"]

def export_anonymized_data():
    """Export anonymized data from database to CSV."""
    
    # Check if database exists
    if not Path(DB_FILE).exists():
        print(f"Error: Database file '{DB_FILE}' not found.")
        print(f"Current directory: {Path.cwd()}")
        return
    
    print("="*70)
    print("EXPORTING ANONYMIZED ACCELERATION DATA")
    print("="*70)
    print(f"\nInput database: {DB_FILE}")
    print(f"Output CSV: {OUTPUT_CSV}")
    print(f"Table: {TABLE_NAME}")
    
    # Connect to database
    print(f"\nConnecting to database...")
    conn = sqlite3.connect(DB_FILE)
    
    try:
        # Read all data from database
        print(f"Reading data from table '{TABLE_NAME}'...")
        df = pd.read_sql_query(f"SELECT * FROM {TABLE_NAME}", conn)
        
        if df.empty:
            print("Error: No data found in table.")
            return
        
        print(f"Loaded {len(df):,} rows, {len(df.columns)} columns")
        
        # Check if 'name' column exists
        if 'name' not in df.columns:
            print("Error: 'name' column not found in database.")
            print(f"Available columns: {', '.join(df.columns[:20])}...")
            return
        
        # Get unique names
        unique_names = df['name'].dropna().unique()
        unique_names = sorted([name for name in unique_names if name != ''])  # Sort for consistency
        
        print(f"\nFound {len(unique_names)} unique athlete names")
        
        # Create name to Subject ID mapping
        name_to_subject = {}
        for idx, name in enumerate(unique_names, start=1):
            subject_id = f"S{idx:03d}"  # S001, S002, ..., S210
            name_to_subject[name] = subject_id
        
        print(f"Created Subject ID mapping:")
        print(f"  First 5: {dict(list(name_to_subject.items())[:5])}")
        print(f"  Last 5: {dict(list(name_to_subject.items())[-5:])}")
        
        # Replace name column with subject column
        print(f"\nReplacing 'name' column with 'subject' column...")
        df['subject'] = df['name'].map(name_to_subject)
        
        # Check for any unmapped names (shouldn't happen, but just in case)
        unmapped = df[df['subject'].isna() & df['name'].notna()]
        if len(unmapped) > 0:
            print(f"  WARNING: {len(unmapped)} rows have unmapped names!")
            print(f"  Sample unmapped names: {unmapped['name'].unique()[:5]}")
            # Fill with placeholder
            df.loc[df['subject'].isna(), 'subject'] = 'UNKNOWN'
        
        # Remove name column
        df = df.drop(columns=['name'])
        
        # Add handedness column based on owner column
        print(f"\nAdding 'handedness' column based on 'owner' column...")
        if 'owner' in df.columns:
            def get_handedness(owner_value):
                if pd.isna(owner_value) or owner_value == '':
                    return None
                owner_str = str(owner_value).upper()
                if 'RH' in owner_str:
                    return 'Right'
                elif 'LH' in owner_str:
                    return 'Left'
                else:
                    return None  # Unknown if neither RH nor LH found
            
            df['handedness'] = df['owner'].apply(get_handedness)
            
            # Report handedness distribution
            handedness_counts = df['handedness'].value_counts()
            print(f"  Handedness distribution:")
            for hand, count in handedness_counts.items():
                print(f"    {hand}: {count:,} rows")
            
            # Check for rows without handedness
            no_handedness = df['handedness'].isna().sum()
            if no_handedness > 0:
                print(f"  ⚠ WARNING: {no_handedness} rows could not be assigned handedness")
        else:
            print(f"  ⚠ WARNING: 'owner' column not found - cannot determine handedness")
            df['handedness'] = None
        
        # Remove specified columns if they exist
        print(f"\nRemoving identifying columns...")
        columns_to_remove = [col for col in COLUMNS_TO_REMOVE if col in df.columns]
        if columns_to_remove:
            print(f"  Removing: {', '.join(columns_to_remove)}")
            df = df.drop(columns=columns_to_remove)
        else:
            print(f"  None of the specified columns found (they may have already been removed)")
        
        # Verify final dimensions
        final_rows = len(df)
        final_cols = len(df.columns)
        final_subjects = df['subject'].nunique()
        
        print(f"\n" + "="*70)
        print("DATA SUMMARY")
        print("="*70)
        print(f"Total rows: {final_rows:,}")
        print(f"Total columns: {final_cols}")
        print(f"Unique subjects: {final_subjects}")
        
        # Verify expected dimensions
        expected_rows = 19840
        expected_cols = 1301
        expected_subjects = 210
        
        print(f"\nExpected dimensions:")
        print(f"  Rows: {expected_rows:,} (actual: {final_rows:,}) {'✓' if final_rows == expected_rows else '✗'}")
        print(f"  Columns: {expected_cols} (actual: {final_cols}) {'✓' if final_cols == expected_cols else '✗'}")
        print(f"  Subjects: {expected_subjects} (actual: {final_subjects}) {'✓' if final_subjects == expected_subjects else '✗'}")
        
        if final_rows != expected_rows or final_cols != expected_cols or final_subjects != expected_subjects:
            print(f"\n⚠ WARNING: Dimensions don't match expected values!")
            response = input("Continue with export anyway? (y/n): ")
            if response.lower() != 'y':
                print("Export cancelled.")
                return
        
        # Write base CSV (all data)
        print(f"\nWriting to CSV: {OUTPUT_CSV}...")
        df.to_csv(OUTPUT_CSV, index=False)
        
        print(f"✓ Successfully exported {final_rows:,} rows to {OUTPUT_CSV}")
        print(f"  File size: {Path(OUTPUT_CSV).stat().st_size / 1024 / 1024:.2f} MB")
        
        # Create filtered datasets
        print(f"\n" + "="*70)
        print("CREATING AGE-FILTERED DATASETS")
        print("="*70)
        
        df_16_plus = None
        df_15_plus = None
        subjects_16_plus = 0
        subjects_15_plus = 0
        
        # Filter 1: age_at_collection >= 16
        if 'age_at_collection' in df.columns:
            print(f"\nFiltering for age_at_collection >= 16...")
            df_16_plus = df[df['age_at_collection'] >= 16].copy()
            
            # Also remove rows where age_at_collection is NA (we want only those with valid age >= 16)
            initial_count = len(df_16_plus)
            df_16_plus = df_16_plus[df_16_plus['age_at_collection'].notna()]
            removed_na = initial_count - len(df_16_plus)
            
            if removed_na > 0:
                print(f"  Removed {removed_na} rows with missing age_at_collection")
            
            rows_removed_16 = final_rows - len(df_16_plus)
            subjects_16_plus = df_16_plus['subject'].nunique()
            
            print(f"  Original rows: {final_rows:,}")
            print(f"  Rows removed (< 16): {rows_removed_16:,}")
            print(f"  Remaining rows: {len(df_16_plus):,}")
            print(f"  Unique subjects: {subjects_16_plus}")
            
            # Write CSV
            print(f"\nWriting to CSV: {OUTPUT_CSV_16_PLUS}...")
            df_16_plus.to_csv(OUTPUT_CSV_16_PLUS, index=False)
            print(f"✓ Successfully exported {len(df_16_plus):,} rows to {OUTPUT_CSV_16_PLUS}")
            print(f"  File size: {Path(OUTPUT_CSV_16_PLUS).stat().st_size / 1024 / 1024:.2f} MB")
        else:
            print(f"\n⚠ WARNING: 'age_at_collection' column not found in database!")
            print(f"  Skipping {OUTPUT_CSV_16_PLUS}")
        
        # Filter 2: age >= 15
        if 'age' in df.columns:
            print(f"\nFiltering for age >= 15...")
            df_15_plus = df[df['age'] >= 15].copy()
            
            # Also remove rows where age is NA (we want only those with valid age >= 15)
            initial_count = len(df_15_plus)
            df_15_plus = df_15_plus[df_15_plus['age'].notna()]
            removed_na = initial_count - len(df_15_plus)
            
            if removed_na > 0:
                print(f"  Removed {removed_na} rows with missing age")
            
            rows_removed_15 = final_rows - len(df_15_plus)
            subjects_15_plus = df_15_plus['subject'].nunique()
            
            print(f"  Original rows: {final_rows:,}")
            print(f"  Rows removed (< 15): {rows_removed_15:,}")
            print(f"  Remaining rows: {len(df_15_plus):,}")
            print(f"  Unique subjects: {subjects_15_plus}")
            
            # Write CSV
            print(f"\nWriting to CSV: {OUTPUT_CSV_15_PLUS}...")
            df_15_plus.to_csv(OUTPUT_CSV_15_PLUS, index=False)
            print(f"✓ Successfully exported {len(df_15_plus):,} rows to {OUTPUT_CSV_15_PLUS}")
            print(f"  File size: {Path(OUTPUT_CSV_15_PLUS).stat().st_size / 1024 / 1024:.2f} MB")
        else:
            print(f"\n⚠ WARNING: 'age' column not found in database!")
            print(f"  Skipping {OUTPUT_CSV_15_PLUS}")
        
        # Show sample of anonymized data
        print(f"\n" + "="*70)
        print("SAMPLE DATA (first 5 rows)")
        print("="*70)
        print(df[['subject', 'owner', 'variable', 'component']].head() if all(col in df.columns for col in ['subject', 'owner', 'variable', 'component']) else df.head())
        
        # Show subject distribution
        print(f"\n" + "="*70)
        print("SUBJECT DISTRIBUTION")
        print("="*70)
        subject_counts = df['subject'].value_counts().sort_index()
        print(f"Rows per subject:")
        print(f"  Min: {subject_counts.min()}")
        print(f"  Max: {subject_counts.max()}")
        print(f"  Mean: {subject_counts.mean():.1f}")
        print(f"\nFirst 10 subjects:")
        for subject, count in subject_counts.head(10).items():
            print(f"  {subject}: {count} rows")
        
        print(f"\n" + "="*70)
        print("EXPORT SUMMARY")
        print("="*70)
        print(f"\nCreated CSV files:")
        print(f"  1. {OUTPUT_CSV} - All data ({final_rows:,} rows, {final_subjects} subjects)")
        if df_16_plus is not None:
            print(f"  2. {OUTPUT_CSV_16_PLUS} - Age at collection >= 16 ({len(df_16_plus):,} rows, {subjects_16_plus} subjects)")
        if df_15_plus is not None:
            print(f"  3. {OUTPUT_CSV_15_PLUS} - Age >= 15 ({len(df_15_plus):,} rows, {subjects_15_plus} subjects)")
        print(f"\n" + "="*70)
        print("EXPORT COMPLETE")
        print("="*70)
        
    except Exception as e:
        print(f"\nError during export: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()

if __name__ == "__main__":
    export_anonymized_data()

