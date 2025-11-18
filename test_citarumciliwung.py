#!/usr/bin/env python3
"""
Simple test script for citarumciliwung database
Test the updated pemutakhiran baseline logic
"""

import psycopg2
import json

DB_CONFIG = {
    'host': '52.74.112.75',
    'port': 5432,
    'user': 'pg',
    'password': '~nagha2025yasha@~'
}

def check_qc_status_all_true(qcstatus):
    """Check if all QC status items are valid"""
    if not qcstatus or not isinstance(qcstatus, list):
        return False
    
    valid_values = {
        'true',
        'sesuai', 
        'lengkap',
        'sudah sesuai semua',
        'sudah sesuai Sebagian',
        'tidak terdapat gap/overlap poligon'
    }
    
    for item in qcstatus:
        if not isinstance(item, dict):
            continue
        
        title = item.get('title', '')
        value = item.get('value', '')
        
        # Skip Remarks - it can have any value
        if 'remark' in title.lower():
            continue
        
        # All other items must have valid values
        if value not in valid_values:
            return False
    
    return True

def main():
    # Connect to citarumciliwung
    conn = psycopg2.connect(
        **DB_CONFIG,
        database='citarumciliwung'
    )
    cursor = conn.cursor()
    
    # Get columns
    cursor.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = 'existing_2025_qc' 
          AND table_schema = 'public'
          AND column_name != 'ogc_fid'
        ORDER BY ordinal_position
    """)
    
    columns = [row[0] for row in cursor.fetchall()]
    print(f"Total columns: {len(columns)}")
    print(f"Columns: {', '.join(columns[:10])}...")
    
    # Get select columns (exclude ogc_fid and geometry)
    select_columns = [col for col in columns if col != 'geometry']
    columns_str = ', '.join(select_columns)
    
    # Build query
    query = f"""
        SELECT DISTINCT ON (ST_AsText(geometry))
            ST_AsBinary(geometry) as geom_binary,
            {columns_str},
            ogc_fid
        FROM public.existing_2025_qc
        WHERE qcstatus IS NOT NULL
          AND geometry IS NOT NULL
        ORDER BY ST_AsText(geometry), ogc_fid DESC
    """
    
    cursor.execute(query)
    records = cursor.fetchall()
    
    print(f"\nTotal DISTINCT ON records: {len(records)}")
    
    # Find qcstatus index
    qcstatus_idx = None
    for i, col in enumerate(select_columns, start=1):
        if col == 'qcstatus':
            qcstatus_idx = i
            break
    
    print(f"qcstatus_idx: {qcstatus_idx}")
    
    # Validate
    valid_count = 0
    for record in records:
        qcstatus = record[qcstatus_idx]
        if check_qc_status_all_true(qcstatus):
            valid_count += 1
    
    print(f"Valid records (all QC true/sesuai/lengkap): {valid_count}")
    
    # Show first valid record
    for record in records:
        qcstatus = record[qcstatus_idx]
        if check_qc_status_all_true(qcstatus):
            ogc_fid = record[-1]  # Last column is ogc_fid
            print(f"\nFirst valid record:")
            print(f"  ogc_fid: {ogc_fid}")
            print(f"  QC Status:")
            for item in qcstatus:
                if 'remark' not in item.get('title', '').lower():
                    print(f"    - {item.get('title')}: {item.get('value')}")
            break
    
    cursor.close()
    conn.close()

if __name__ == '__main__':
    main()
