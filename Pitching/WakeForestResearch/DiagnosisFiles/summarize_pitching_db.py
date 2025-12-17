"""
Summarize pitching_accel_data_v1.db by extracting values at specific events.
Creates a new database (pitching_accel_data_sum.db) with event-specific values.

For each row (variable-component combination), extracts:
- Value at Footstrike (@footstrike)
- Value at Max Shoulder Rotation (@max_shoulder_rot)
- Value at Release (@release)
- Value at Max IR (@max_ir)
- Maximum value across all frames (@max)
"""

import sqlite3
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

# Configuration
INPUT_DB = "pitching_accel_data_v1.db"
OUTPUT_DB = "pitching_accel_data_sum.db"

def time_to_frame(event_time, time_start, time_end, frames):
    """
    Convert event time to frame number.
    Uses linear interpolation between time_start and time_end.
    """
    if pd.isna(event_time) or pd.isna(time_start) or pd.isna(time_end) or frames <= 1:
        return None
    
    # Handle case where time is outside the range
    if event_time < time_start:
        return 1
    if event_time > time_end:
        return frames
    
    # Linear interpolation
    if time_end == time_start:
        return 1
    
    # Calculate frame (1-indexed)
    frame_float = 1 + (event_time - time_start) / (time_end - time_start) * (frames - 1)
    frame = int(round(frame_float))
    
    # Ensure frame is within valid range
    frame = max(1, min(frame, frames))
    
    return frame

def get_value_at_frame(row, frame_num):
    """Get value at a specific frame number from the row's value columns."""
    if frame_num is None or frame_num < 1:
        return None
    
    value_col = f"value_{frame_num}"
    if value_col in row.index:
        val = row[value_col]
        return val if pd.notna(val) else None
    return None

def get_max_value(row, frames):
    """Get maximum absolute value across all frames."""
    if frames is None or frames < 1:
        return None
    
    values = []
    for i in range(1, frames + 1):
        value_col = f"value_{i}"
        if value_col in row.index:
            val = row[value_col]
            if pd.notna(val):
                values.append(val)
    
    if not values:
        return None
    
    # Return maximum absolute value (and also store the signed max)
    abs_values = [abs(v) for v in values]
    max_abs_idx = abs_values.index(max(abs_values))
    return values[max_abs_idx]

def summarize_database():
    """Create summarized database with event-specific values."""
    
    # Check if input database exists
    if not Path(INPUT_DB).exists():
        print(f"Error: Input database '{INPUT_DB}' not found.")
        print(f"Current directory: {Path.cwd()}")
        return False
    
    print(f"Connecting to input database: {INPUT_DB}")
    input_conn = sqlite3.connect(INPUT_DB)
    
    try:
        # Get table names
        cursor = input_conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        
        if not tables:
            print("No tables found in database.")
            return False
        
        # Find the main table (likely 'pitches')
        main_table = None
        for table_name, in tables:
            if 'pitch' in table_name.lower():
                main_table = table_name
                break
        
        if not main_table:
            main_table = tables[0][0]
        
        print(f"Reading table: {main_table}")
        
        # Read all data
        df = pd.read_sql_query(f"SELECT * FROM {main_table}", input_conn)
        
        if df.empty:
            print("No data found in table.")
            return False
        
        print(f"Loaded {len(df):,} rows")
        
        # Find all columns
        all_columns = df.columns.tolist()
        
        # Find where value_1 starts (keep everything before it)
        value_1_idx = None
        for i, col in enumerate(all_columns):
            if col == 'value_1':
                value_1_idx = i
                break
        
        if value_1_idx is None:
            print("Error: 'value_1' column not found.")
            print("Available columns:", all_columns[:20])
            return False
        
        # Columns to keep (everything up to and including metadata columns before value_1)
        # We'll keep everything up to value_1, but exclude the value_* columns
        keep_columns = all_columns[:value_1_idx]
        
        # Also keep time_start, time_end, frames if they exist (they might be after value_1)
        metadata_cols = ['time_start', 'time_end', 'frames']
        for col in metadata_cols:
            if col in all_columns and col not in keep_columns:
                keep_columns.append(col)
        
        print(f"\nKeeping {len(keep_columns)} metadata columns")
        print(f"Processing time series data from value_1 onwards...")
        
        # Find event time columns
        event_time_cols = {
            'footstrike': 'event_footstrike_time',
            'max_shoulder_rot': 'event_max_shoulder_rot_time',
            'release': 'event_release_time',
            'max_ir': 'max_ir_time'
        }
        
        # Check which event columns exist
        available_events = {}
        for event_name, col_name in event_time_cols.items():
            if col_name in df.columns:
                available_events[event_name] = col_name
            else:
                print(f"Warning: Event column '{col_name}' not found, skipping {event_name}")
        
        if not available_events:
            print("Error: No event time columns found.")
            return False
        
        print(f"\nFound {len(available_events)} event time columns:")
        for event_name, col_name in available_events.items():
            print(f"  - {event_name}: {col_name}")
        
        # Process each row
        print(f"\nProcessing {len(df):,} rows...")
        summarized_rows = []
        
        for idx, row in df.iterrows():
            if (idx + 1) % 1000 == 0:
                print(f"  Processed {idx + 1:,} rows...")
            
            # Start with metadata columns
            new_row = row[keep_columns].copy()
            
            # Get time series metadata
            time_start = row.get('time_start')
            time_end = row.get('time_end')
            frames = row.get('frames')
            
            # Convert frames to int if it's not already
            if pd.notna(frames):
                try:
                    frames = int(frames)
                except (ValueError, TypeError):
                    frames = None
            
            # Extract values at each event
            for event_name, event_col in available_events.items():
                event_time = row.get(event_col)
                
                if pd.notna(event_time) and pd.notna(time_start) and pd.notna(time_end) and frames:
                    # Convert time to frame number
                    frame_num = time_to_frame(event_time, time_start, time_end, frames)
                    
                    # Get value at that frame
                    value = get_value_at_frame(row, frame_num)
                    new_row[f"@{event_name}"] = value
                else:
                    new_row[f"@{event_name}"] = None
            
            # Get maximum value
            if frames:
                max_value = get_max_value(row, frames)
                new_row["@max"] = max_value
            else:
                new_row["@max"] = None
            
            summarized_rows.append(new_row)
        
        # Create DataFrame from summarized rows
        df_sum = pd.DataFrame(summarized_rows)
        
        print(f"\nSummarized data: {len(df_sum):,} rows, {len(df_sum.columns)} columns")
        
        # Create output database
        print(f"\nWriting to output database: {OUTPUT_DB}")
        if Path(OUTPUT_DB).exists():
            print(f"Warning: {OUTPUT_DB} already exists. It will be overwritten.")
        
        output_conn = sqlite3.connect(OUTPUT_DB)
        df_sum.to_sql(main_table, output_conn, if_exists='replace', index=False)
        
        # Create indexes
        print("Creating indexes...")
        try:
            output_conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{main_table}_owner ON {main_table}(owner)")
            output_conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{main_table}_variable ON {main_table}(variable)")
            output_conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{main_table}_component ON {main_table}(component)")
            print("  Indexes created")
        except Exception as e:
            print(f"  Warning: Could not create all indexes: {e}")
        
        output_conn.close()
        
        # Show summary statistics
        print("\n" + "="*70)
        print("SUMMARY STATISTICS")
        print("="*70)
        
        for event_name in available_events.keys():
            col_name = f"@{event_name}"
            if col_name in df_sum.columns:
                non_null = df_sum[col_name].notna().sum()
                null_count = df_sum[col_name].isna().sum()
                print(f"\n{col_name}:")
                print(f"  Non-NULL: {non_null:,} ({non_null/len(df_sum)*100:.2f}%)")
                print(f"  NULL: {null_count:,} ({null_count/len(df_sum)*100:.2f}%)")
        
        if "@max" in df_sum.columns:
            non_null = df_sum["@max"].notna().sum()
            null_count = df_sum["@max"].isna().sum()
            print(f"\n@max:")
            print(f"  Non-NULL: {non_null:,} ({non_null/len(df_sum)*100:.2f}%)")
            print(f"  NULL: {null_count:,} ({null_count/len(df_sum)*100:.2f}%)")
        
        print("\n" + "="*70)
        print("SUMMARIZATION COMPLETE")
        print("="*70)
        print(f"\nInput:  {INPUT_DB} ({len(df):,} rows)")
        print(f"Output: {OUTPUT_DB} ({len(df_sum):,} rows)")
        print(f"\nNew columns added:")
        for event_name in available_events.keys():
            print(f"  - @{event_name}")
        print(f"  - @max")
        
        return True
        
    except Exception as e:
        print(f"\nError summarizing database: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        input_conn.close()

if __name__ == "__main__":
    print("="*70)
    print("PITCHING DATA SUMMARIZATION")
    print("="*70)
    print(f"\nInput database:  {INPUT_DB}")
    print(f"Output database: {OUTPUT_DB}")
    print("\nThis script will:")
    print("  1. Read all rows from the input database")
    print("  2. Extract values at event times (Footstrike, Max Shoulder Rot, Release, Max IR)")
    print("  3. Extract maximum values for each variable-component")
    print("  4. Create a new summarized database")
    print("\n" + "="*70 + "\n")
    
    success = summarize_database()
    
    if success:
        print("\n✓ Summarization completed successfully!")
    else:
        print("\n✗ Summarization failed.")
        exit(1)

