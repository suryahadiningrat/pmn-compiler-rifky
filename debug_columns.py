#!/usr/bin/env python3
import psycopg2

conn = psycopg2.connect(
    host='52.74.112.75',
    port=5432,
    user='pg',
    password='~nagha2025yasha@~',
    database='bpdastesting'
)
cursor = conn.cursor()

# Get columns like script does
cursor.execute("""
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'public'
    AND table_name = 'existing_2025_qc'
    AND column_name != 'ogc_fid'
    ORDER BY ordinal_position
""")

columns = [row[0] for row in cursor.fetchall()]
select_columns = [col for col in columns if col != 'geometry']

print(f"columns ({len(columns)}): {columns[:10]}...")
print(f"\nselect_columns ({len(select_columns)}): {select_columns[:10]}...")
print(f"\nInsert columns would be: ['geometry'] + select_columns = {['geometry'] + select_columns[:5]}...")

# Check if ogc_fid is in any of these
print(f"\n'ogc_fid' in columns: {'ogc_fid' in columns}")
print(f"'ogc_fid' in select_columns: {'ogc_fid' in select_columns}")

cursor.close()
conn.close()
