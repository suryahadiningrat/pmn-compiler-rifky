
import psycopg2
import json

DB_CONFIG = {
    'host': '52.74.112.75',
    'port': 5432,
    'user': 'pg',
    'password': '~nagha2025yasha@~'
}

def check_schema():
    try:
        conn = psycopg2.connect(
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            database='citarumciliwung'
        )
        cursor = conn.cursor()
        
        # Check tables
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name LIKE 'existing_%'
        """)
        tables = cursor.fetchall()
        print("Tables found:", [t[0] for t in tables])
        
        # Check columns in existing_2025
        print("\nColumns in existing_2025:")
        cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'existing_2025'
        """)
        cols = cursor.fetchall()
        for c in cols:
            print(f" - {c[0]} ({c[1]})")
            
        # Check columns in existing_2025_qc
        print("\nColumns in existing_2025_qc:")
        cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'existing_2025_qc'
        """)
        cols = cursor.fetchall()
        for c in cols:
            print(f" - {c[0]} ({c[1]})")

        # Check sample data from existing_2025_qc
        print("\nSample qcstatus from existing_2025_qc:")
        try:
            cursor.execute("SELECT qcstatus FROM existing_2025_qc LIMIT 1")
            row = cursor.fetchone()
            if row:
                print(row[0])
        except Exception as e:
            print(f"Error reading qcstatus: {e}")

        conn.close()
    except Exception as e:
        print(f"Connection failed: {e}")

if __name__ == "__main__":
    check_schema()
