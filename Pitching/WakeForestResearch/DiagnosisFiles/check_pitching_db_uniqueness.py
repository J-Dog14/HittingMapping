"""
Analyze pitching_accel_data_v1.db for uniqueness and duplicates.
Checks if trials are unique and identifies any repeated data.
"""

import sqlite3
import pandas as pd
from pathlib import Path
from collections import Counter

# Configuration
DB_FILE = "pitching_accel_data_v1.db"

def analyze_uniqueness():
    """Analyze database for uniqueness and duplicates."""
    
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
        
        # Read all data
        df = pd.read_sql_query(f"SELECT * FROM {main_table}", conn)
        
        if df.empty:
            print("No data found in table.")
            return
        
        total_rows = len(df)
        print(f"\nTotal rows: {total_rows:,}")
        
        # Get column names
        columns = df.columns.tolist()
        print(f"Total columns: {len(columns)}")
        
        # Identify key identifying columns
        key_columns = []
        potential_keys = ['owner', 'variable', 'component', 'name', 'athlete_id', 
                         'source_data_file', 'source_file']
        
        for col in potential_keys:
            if col in columns:
                key_columns.append(col)
        
        print(f"\nKey identifying columns found: {key_columns}")
        
        # Analyze uniqueness by different combinations
        print("\n" + "="*70)
        print("UNIQUENESS ANALYSIS")
        print("="*70)
        
        # 1. Unique owners (trials)
        if 'owner' in columns:
            unique_owners = df['owner'].nunique()
            total_owners = df['owner'].count()
            owner_counts = df['owner'].value_counts()
            
            print(f"\n1. OWNER (Trial) Analysis:")
            print(f"   Unique owners: {unique_owners}")
            print(f"   Total owner entries: {total_owners:,}")
            print(f"   Average rows per owner: {total_owners/unique_owners:.2f}")
            
            print(f"\n   Owner frequency distribution:")
            print(f"   {owner_counts.head(10).to_string()}")
            if len(owner_counts) > 10:
                print(f"   ... and {len(owner_counts) - 10} more owners")
        
        # 2. Unique variable-component combinations
        if 'variable' in columns and 'component' in columns:
            df['var_comp'] = df['variable'].astype(str) + '_' + df['component'].astype(str)
            unique_var_comp = df['var_comp'].nunique()
            
            print(f"\n2. VARIABLE-COMPONENT Combinations:")
            print(f"   Unique combinations: {unique_var_comp}")
            
            var_comp_counts = df['var_comp'].value_counts()
            print(f"\n   Most common variable-component combinations:")
            print(f"   {var_comp_counts.head(10).to_string()}")
        
        # 3. Check for duplicate DATA VALUES (value_1, value_2, etc.)
        value_columns = [col for col in columns if col.startswith('value_')]
        
        if value_columns:
            print(f"\n3. DATA VALUE DUPLICATES (checking {len(value_columns)} value columns):")
            
            # Create a hash of the value columns for each row
            print("   Creating data fingerprints...")
            df['data_fingerprint'] = df[value_columns].apply(
                lambda row: hash(tuple(row.values)), axis=1
            )
            
            # Check for duplicate data fingerprints
            fingerprint_counts = df['data_fingerprint'].value_counts()
            duplicate_fingerprints = fingerprint_counts[fingerprint_counts > 1]
            
            if len(duplicate_fingerprints) > 0:
                print(f"\n   ⚠ WARNING: Found {len(duplicate_fingerprints)} sets of duplicate data!")
                print(f"   Total rows with duplicate data: {duplicate_fingerprints.sum():,}")
                
                # Show examples
                print(f"\n   Sample duplicate data sets:")
                for fingerprint, count in list(duplicate_fingerprints.head(10).items()):
                    dup_rows = df[df['data_fingerprint'] == fingerprint]
                    print(f"\n     Data fingerprint {fingerprint}: appears {count} times")
                    if 'owner' in columns and 'variable' in columns and 'component' in columns:
                        print(f"     Owners: {dup_rows['owner'].unique().tolist()}")
                        print(f"     Variables: {dup_rows['variable'].unique().tolist()}")
                        print(f"     Components: {dup_rows['component'].unique().tolist()}")
            else:
                print(f"   ✓ No duplicate data values found - all time series are unique")
            
            # Also check owner-variable-component combinations (expected to have multiple rows)
            if 'owner' in columns and 'variable' in columns and 'component' in columns:
                df['owner_var_comp'] = (df['owner'].astype(str) + '_' + 
                                        df['variable'].astype(str) + '_' + 
                                        df['component'].astype(str))
                
                unique_combos = df['owner_var_comp'].nunique()
                combo_counts = df['owner_var_comp'].value_counts()
                
                print(f"\n   Owner-Variable-Component combinations:")
                print(f"   Unique combinations: {unique_combos:,}")
                print(f"   Expected: Each owner should have {len(value_columns)} variable-component rows")
                
                # Check if each owner-variable-component has unique data
                owner_var_comp_with_dupes = []
                for combo in combo_counts.index:
                    combo_rows = df[df['owner_var_comp'] == combo]
                    if combo_rows['data_fingerprint'].nunique() < len(combo_rows):
                        owner_var_comp_with_dupes.append(combo)
                
                if owner_var_comp_with_dupes:
                    print(f"\n   ⚠ WARNING: {len(owner_var_comp_with_dupes)} owner-variable-component combinations have duplicate data!")
                    print(f"   This means the same data appears multiple times for the same trial/variable.")
                else:
                    print(f"   ✓ Each owner-variable-component combination has unique data")
        
        # 4. Check for exact duplicate rows
        print(f"\n4. EXACT DUPLICATE ROWS:")
        # Exclude value columns for comparison (they're too many)
        non_value_cols = [col for col in columns if not col.startswith('value_')]
        df_subset = df[non_value_cols]
        
        duplicates_exact = df_subset.duplicated()
        num_exact_duplicates = duplicates_exact.sum()
        
        print(f"   Exact duplicate rows (excluding value columns): {num_exact_duplicates:,}")
        
        if num_exact_duplicates > 0:
            print(f"   ⚠ WARNING: Found {num_exact_duplicates:,} exact duplicate rows!")
            
            # Show some examples
            duplicate_rows = df_subset[duplicates_exact]
            print(f"\n   Sample duplicate rows:")
            print(duplicate_rows.head(10).to_string())
        
        # 5. Analyze by athlete/name - check for name repetition issues
        if 'name' in columns:
            unique_names = df['name'].nunique()
            name_counts = df['name'].value_counts()
            
            print(f"\n5. ATHLETE NAME Analysis:")
            print(f"   Unique athlete names: {unique_names}")
            print(f"   Total rows: {total_rows:,}")
            print(f"   Average rows per name: {total_rows/unique_names:.2f}")
            
            # Check which names appear with which owners
            if 'owner' in columns:
                name_owner_pairs = df[['name', 'owner']].drop_duplicates()
                name_owner_counts = name_owner_pairs.groupby('name')['owner'].nunique()
                
                print(f"\n   Owners per athlete name:")
                print(f"   {name_owner_counts.value_counts().sort_index().to_string()}")
                
                # Find names that appear with multiple owners
                names_with_multiple_owners = name_owner_counts[name_owner_counts > 1]
                
                if len(names_with_multiple_owners) > 0:
                    print(f"\n   ⚠ WARNING: {len(names_with_multiple_owners)} names appear with multiple owners!")
                    print(f"   This could indicate data duplication or incorrect matching.")
                    print(f"\n   Names with multiple owners:")
                    for name, owner_count in names_with_multiple_owners.head(10).items():
                        owners = name_owner_pairs[name_owner_pairs['name'] == name]['owner'].unique()
                        print(f"     {name}: {owner_count} owners")
                        print(f"       Owners: {list(owners)[:5]}{'...' if len(owners) > 5 else ''}")
                    if len(names_with_multiple_owners) > 10:
                        print(f"     ... and {len(names_with_multiple_owners) - 10} more")
                else:
                    print(f"\n   ✓ Each name appears with exactly one owner (expected behavior)")
            
            print(f"\n   Most common names by row count:")
            print(f"   {name_counts.head(10).to_string()}")
        
        # 6. Check for data integrity issues
        print(f"\n6. DATA INTEGRITY CHECKS:")
        
        # Check if same owner appears with different athletes
        if 'owner' in columns and 'name' in columns:
            owner_name_pairs = df[['owner', 'name']].drop_duplicates()
            owner_counts = owner_name_pairs['owner'].value_counts()
            mismatched_owners = owner_counts[owner_counts > 1]
            
            if len(mismatched_owners) > 0:
                print(f"   ⚠ WARNING: {len(mismatched_owners)} owners appear with multiple athlete names!")
                print(f"   Sample mismatches:")
                for owner in mismatched_owners.head(5).index:
                    names = owner_name_pairs[owner_name_pairs['owner'] == owner]['name'].unique()
                    print(f"     {owner}: {list(names)}")
            else:
                print(f"   ✓ Each owner is associated with exactly one athlete name")
        
        # Check for missing key fields
        if 'owner' in columns:
            null_owners = df['owner'].isna().sum()
            if null_owners > 0:
                print(f"   ⚠ WARNING: {null_owners} rows have NULL owner")
        
        if 'variable' in columns:
            null_vars = df['variable'].isna().sum()
            if null_vars > 0:
                print(f"   ⚠ WARNING: {null_vars} rows have NULL variable")
        
        if 'component' in columns:
            null_comps = df['component'].isna().sum()
            if null_comps > 0:
                print(f"   ⚠ WARNING: {null_comps} rows have NULL component")
        
        # 7. Expected vs Actual row counts
        print(f"\n7. EXPECTED vs ACTUAL ROW COUNTS:")
        
        if 'owner' in columns and 'variable' in columns and 'component' in columns:
            # Expected: each owner should have the same number of variable-component combinations
            owner_var_comp_counts = df.groupby('owner')['owner_var_comp'].nunique()
            
            print(f"   Variable-component combinations per owner:")
            print(f"   Min: {owner_var_comp_counts.min()}")
            print(f"   Max: {owner_var_comp_counts.max()}")
            print(f"   Mean: {owner_var_comp_counts.mean():.2f}")
            print(f"   Std Dev: {owner_var_comp_counts.std():.2f}")
            
            if owner_var_comp_counts.nunique() > 1:
                print(f"\n   ⚠ WARNING: Owners have different numbers of variable-component combinations!")
                print(f"   This suggests inconsistent data extraction.")
                print(f"\n   Distribution:")
                print(owner_var_comp_counts.value_counts().sort_index().to_string())
            else:
                print(f"   ✓ All owners have the same number of variable-component combinations")
        
        # 8. Generate detailed duplicate DATA report
        print(f"\n" + "="*70)
        print("DETAILED DUPLICATE DATA REPORT")
        print("="*70)
        
        if value_columns and 'data_fingerprint' in df.columns:
            # Find rows with duplicate data fingerprints
            fingerprint_counts = df['data_fingerprint'].value_counts()
            duplicate_fingerprints = fingerprint_counts[fingerprint_counts > 1]
            
            if len(duplicate_fingerprints) > 0:
                print(f"\nFound {len(duplicate_fingerprints)} sets of duplicate data values:")
                print(f"Total rows with duplicate data: {duplicate_fingerprints.sum():,}")
                
                duplicate_df = df[df['data_fingerprint'].isin(duplicate_fingerprints.index)].copy()
                
                # Group by data fingerprint
                grouped = duplicate_df.groupby('data_fingerprint')
                
                print(f"\nDuplicate data groups ({len(grouped)}):")
                for fingerprint, group in list(grouped)[:10]:
                    print(f"\n  Data Fingerprint: {fingerprint}")
                    print(f"  Count: {len(group)} rows")
                    
                    if 'owner' in columns:
                        print(f"  Owners: {group['owner'].unique().tolist()}")
                    if 'variable' in columns and 'component' in columns:
                        print(f"  Variable-Component: {group[['variable', 'component']].drop_duplicates().to_string(index=False)}")
                    if 'name' in columns:
                        print(f"  Athlete Names: {group['name'].unique().tolist()}")
                    
                    # Show if metadata differs
                    metadata_cols = ['time_start', 'time_end', 'frames', 'source_data_file']
                    metadata_cols = [c for c in metadata_cols if c in group.columns]
                    if metadata_cols:
                        metadata_diffs = group[metadata_cols].nunique()
                        if metadata_diffs.sum() > len(metadata_cols):
                            print(f"  ⚠ Metadata differs between rows")
                        else:
                            print(f"  ✓ Metadata is identical")
                
                if len(grouped) > 10:
                    print(f"\n  ... and {len(grouped) - 10} more duplicate groups")
                
                # Save duplicate report
                output_file = "duplicate_data_report.csv"
                duplicate_df.to_csv(output_file, index=False)
                print(f"\nFull duplicate data report saved to: {output_file}")
            else:
                print(f"\n✓ No duplicate data values found - all time series data is unique")
        
        # Summary
        print(f"\n" + "="*70)
        print("SUMMARY")
        print("="*70)
        
        issues_found = []
        if value_columns and 'data_fingerprint' in df.columns:
            fingerprint_counts = df['data_fingerprint'].value_counts()
            duplicate_fingerprints = fingerprint_counts[fingerprint_counts > 1]
            if len(duplicate_fingerprints) > 0:
                issues_found.append(f"Duplicate data values: {len(duplicate_fingerprints)} sets, {duplicate_fingerprints.sum():,} total rows")
        
        if num_exact_duplicates > 0:
            issues_found.append(f"Exact duplicate rows: {num_exact_duplicates:,}")
        
        if 'name' in columns and 'owner' in columns:
            name_owner_pairs = df[['name', 'owner']].drop_duplicates()
            name_owner_counts = name_owner_pairs.groupby('name')['owner'].nunique()
            names_with_multiple_owners = name_owner_counts[name_owner_counts > 1]
            if len(names_with_multiple_owners) > 0:
                issues_found.append(f"Athlete names appearing with multiple owners: {len(names_with_multiple_owners)}")
        
        if 'owner' in columns and 'name' in columns and len(mismatched_owners) > 0:
            issues_found.append(f"Owners with multiple athlete names: {len(mismatched_owners)}")
        
        if issues_found:
            print(f"\n⚠ ISSUES FOUND:")
            for issue in issues_found:
                print(f"  - {issue}")
        else:
            print(f"\n✓ No major issues detected. Data appears to be unique.")
        
        print(f"\nTotal rows analyzed: {total_rows:,}")
        if 'owner' in columns:
            print(f"Unique owners (trials): {unique_owners}")
        if 'variable' in columns and 'component' in columns:
            print(f"Unique variable-component combinations: {unique_var_comp}")
        
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
    print("="*70)
    print("PITCHING DATABASE UNIQUENESS ANALYSIS")
    print("="*70)
    analyze_uniqueness()

