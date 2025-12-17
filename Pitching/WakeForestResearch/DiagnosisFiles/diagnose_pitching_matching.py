"""
Diagnose how session.xml and session_data.xml files are being matched.
Helps identify why multiple athlete names are being assigned to the same owners.
"""

import os
from pathlib import Path
from collections import defaultdict

# Configuration
DATA_ROOT = "H:/Pitching/Data"

def diagnose_matching():
    """Diagnose the matching between session.xml and session_data.xml files."""
    
    data_path = Path(DATA_ROOT)
    
    if not data_path.exists():
        print(f"Error: Data directory '{DATA_ROOT}' not found.")
        return
    
    print("="*70)
    print("PITCHING DATA MATCHING DIAGNOSIS")
    print("="*70)
    print(f"\nScanning directory: {DATA_ROOT}")
    
    # Find all session.xml and session_data.xml files
    session_files = list(data_path.rglob("session.xml"))
    session_data_files = list(data_path.rglob("session_data.xml"))
    
    print(f"\nFound {len(session_files)} session.xml files")
    print(f"Found {len(session_data_files)} session_data.xml files")
    
    # Analyze directory structure
    print("\n" + "="*70)
    print("DIRECTORY STRUCTURE ANALYSIS")
    print("="*70)
    
    # Group by directory
    session_by_dir = defaultdict(list)
    session_data_by_dir = defaultdict(list)
    
    for sf in session_files:
        dir_path = str(sf.parent)
        session_by_dir[dir_path].append(sf)
    
    for sdf in session_data_files:
        dir_path = str(sdf.parent)
        session_data_by_dir[dir_path].append(sdf)
    
    # Find directories with both files
    both_files = set(session_by_dir.keys()) & set(session_data_by_dir.keys())
    only_session = set(session_by_dir.keys()) - set(session_data_by_dir.keys())
    only_session_data = set(session_data_by_dir.keys()) - set(session_by_dir.keys())
    
    print(f"\nDirectories with BOTH session.xml and session_data.xml: {len(both_files)}")
    print(f"Directories with ONLY session.xml: {len(only_session)}")
    print(f"Directories with ONLY session_data.xml: {len(only_session_data)}")
    
    # Check for multiple session.xml files in same directory
    print("\n" + "="*70)
    print("MULTIPLE FILES IN SAME DIRECTORY")
    print("="*70)
    
    multiple_session = {k: v for k, v in session_by_dir.items() if len(v) > 1}
    multiple_session_data = {k: v for k, v in session_data_by_dir.items() if len(v) > 1}
    
    if multiple_session:
        print(f"\n⚠ Found {len(multiple_session)} directories with multiple session.xml files:")
        for dir_path, files in list(multiple_session.items())[:10]:
            print(f"\n  {dir_path}:")
            for f in files:
                print(f"    - {f.name}")
        if len(multiple_session) > 10:
            print(f"  ... and {len(multiple_session) - 10} more")
    
    if multiple_session_data:
        print(f"\n⚠ Found {len(multiple_session_data)} directories with multiple session_data.xml files:")
        for dir_path, files in list(multiple_session_data.items())[:10]:
            print(f"\n  {dir_path}:")
            for f in files:
                print(f"    - {f.name}")
        if len(multiple_session_data) > 10:
            print(f"  ... and {len(multiple_session_data) - 10} more")
    
    # Analyze owner names in session_data.xml files
    print("\n" + "="*70)
    print("OWNER NAME ANALYSIS")
    print("="*70)
    
    import xml.etree.ElementTree as ET
    
    owner_to_dirs = defaultdict(set)
    owner_counts = defaultdict(int)
    
    print("\nAnalyzing owners in session_data.xml files...")
    for sdf in session_data_files[:100]:  # Sample first 100
        try:
            tree = ET.parse(sdf)
            root = tree.getroot()
            
            if root.tag == 'v3d':
                owners = root.findall('owner')
                for owner in owners:
                    owner_name = owner.get('value')
                    if owner_name:
                        owner_to_dirs[owner_name].add(str(sdf.parent))
                        owner_counts[owner_name] += 1
        except Exception as e:
            pass
    
    # Find owners that appear in multiple directories
    owners_in_multiple_dirs = {k: v for k, v in owner_to_dirs.items() if len(v) > 1}
    
    print(f"\nUnique owners found: {len(owner_to_dirs)}")
    print(f"Owners appearing in multiple directories: {len(owners_in_multiple_dirs)}")
    
    if owners_in_multiple_dirs:
        print(f"\n⚠ Sample owners in multiple directories:")
        for owner, dirs in list(owners_in_multiple_dirs.items())[:10]:
            print(f"\n  {owner}:")
            print(f"    Appears in {len(dirs)} directories:")
            for d in list(dirs)[:5]:
                print(f"      - {d}")
            if len(dirs) > 5:
                print(f"      ... and {len(dirs) - 5} more")
    
    # Check directory depth and structure
    print("\n" + "="*70)
    print("DIRECTORY STRUCTURE PATTERNS")
    print("="*70)
    
    # Sample some directories to understand structure
    sample_dirs = list(both_files)[:20]
    print(f"\nSample directories with both files (first 20):")
    for dir_path in sample_dirs:
        rel_path = Path(dir_path).relative_to(data_path)
        session_files_in_dir = session_by_dir[dir_path]
        session_data_files_in_dir = session_data_by_dir[dir_path]
        
        print(f"\n  {rel_path}:")
        print(f"    session.xml: {len(session_files_in_dir)} file(s)")
        print(f"    session_data.xml: {len(session_data_files_in_dir)} file(s)")
        
        # Try to extract athlete name from session.xml
        if session_files_in_dir:
            try:
                tree = ET.parse(session_files_in_dir[0])
                root = tree.getroot()
                if root.tag == 'Subject':
                    fields = root.find('Fields')
                    if fields is not None:
                        name_elem = fields.find('Name')
                        if name_elem is not None:
                            name = name_elem.text
                            print(f"    Athlete name: {name}")
            except:
                pass
    
    # Summary
    print("\n" + "="*70)
    print("SUMMARY")
    print("="*70)
    print(f"\nTotal athlete folders: {len(list(data_path.iterdir()))}")
    print(f"Directories with session.xml: {len(session_by_dir)}")
    print(f"Directories with session_data.xml: {len(session_data_by_dir)}")
    print(f"Directories with both: {len(both_files)}")
    
    if len(only_session_data) > 0:
        print(f"\n⚠ WARNING: {len(only_session_data)} directories have session_data.xml but NO session.xml")
        print("  These will need to be matched differently or skipped.")
    
    if len(owners_in_multiple_dirs) > 0:
        print(f"\n⚠ WARNING: {len(owners_in_multiple_dirs)} owners appear in multiple directories")
        print("  This suggests owner names are not unique identifiers.")
        print("  Matching should be based on directory location, not owner name.")
    
    print("\n" + "="*70)

if __name__ == "__main__":
    diagnose_matching()

