#!/usr/bin/env python3
"""
PMN Compiler Script
Mengkompilasi data mangrove dari berbagai database BPDAS ke dalam format final
(GeoJSON, Shapefile, GDB, PMTiles) dengan progress tracking.
"""

import os
import sys
import json
import hashlib
import logging
import zipfile
import tempfile
import subprocess
from datetime import datetime
from typing import List, Dict, Any, Optional
import requests
import psycopg2
import psycopg2.extras
import geopandas as gpd
from minio import Minio
from minio.error import S3Error
import pandas as pd
from pathlib import Path

# Konfigurasi Database
DB_CONFIG = {
    'host': '52.74.112.75',
    'port': 5432,
    'user': 'pg',
    'password': '~nagha2025yasha@~'
}

# Konfigurasi MinIO
MINIO_CONFIG = {
    'host': '52.76.171.132:9005',
    'public_host': 'https://api-minio.ptnaghayasha.com',
    'bucket': 'idpm',
    'access_key': 'SvMJ2t6I3QPqEheG9xDd',
    'secret_key': 'F2uZ4W1pwptDzM4DzbGAfT8mH8SZN1ozsyaQYTEF'
}

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pmn_compiler.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class PMNCompiler:
    def __init__(self, process_id: str, year: int):
        self.process_id = process_id
        self.year = year
        self.minio_client = None
        self.temp_dir = None
        self.accessible_bpdas = []  # Add this line
        
        # Initialize MinIO client
        self._init_minio()
        
        # Create temporary directory
        self.temp_dir = tempfile.mkdtemp(prefix=f'pmn_compiler_{year}_')
        logger.info(f"Temporary directory created: {self.temp_dir}")

    def _init_minio(self):
        """Initialize MinIO client"""
        try:
            self.minio_client = Minio(
                MINIO_CONFIG['host'],
                access_key=MINIO_CONFIG['access_key'],
                secret_key=MINIO_CONFIG['secret_key'],
                secure=False
            )
            logger.info("MinIO client initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize MinIO client: {e}")
            raise

    def _get_db_connection(self, database: str = 'postgres') -> psycopg2.extensions.connection:
        """Get database connection"""
        try:
            conn = psycopg2.connect(
                host=DB_CONFIG['host'],
                port=DB_CONFIG['port'],
                user=DB_CONFIG['user'],
                password=DB_CONFIG['password'],
                database=database
            )
            return conn
        except Exception as e:
            logger.error(f"Failed to connect to database {database}: {e}")
            raise

    def _update_progress(self, progress: int, status: str = 'PROCESSING'):
        """Update progress in compiler_status table"""
        try:
            conn = self._get_db_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                UPDATE pmn.compiler_status 
                SET percentage = %s, status = %s, last_update = NOW()
                WHERE id = %s
            """, (progress, status, self.process_id))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"Progress updated: {progress}% - {status}")
        except Exception as e:
            logger.error(f"Failed to update progress: {e}")
            raise

    def _check_minio_connection(self) -> bool:
        """Check MinIO connection and bucket accessibility"""
        logger.info("Checking MinIO connection...")
        
        try:
            # Check if bucket exists
            if not self.minio_client.bucket_exists(MINIO_CONFIG['bucket']):
                logger.error(f"MinIO bucket '{MINIO_CONFIG['bucket']}' does not exist")
                return False
            
            logger.info(f"MinIO bucket '{MINIO_CONFIG['bucket']}' is accessible")
            
            # Try to list objects (minimal operation to verify permissions)
            list(self.minio_client.list_objects(MINIO_CONFIG['bucket'], prefix='test/', recursive=False))
            logger.info("MinIO read/write permissions verified")
            
            return True
            
        except S3Error as e:
            logger.error(f"MinIO S3 error: {e}")
            return False
        except Exception as e:
            logger.error(f"MinIO connection check failed: {e}")
            return False

    def _check_database_connections(self, bpdas_list: List[str]) -> Dict[str, bool]:
        """Check database connections for postgres and all BPDAS databases"""
        logger.info("Checking database connections...")
        
        connection_status = {}
        
        # Check main postgres database
        try:
            conn = self._get_db_connection('postgres')
            cursor = conn.cursor()
            cursor.execute("SELECT version();")
            version = cursor.fetchone()[0]
            cursor.close()
            conn.close()
            
            connection_status['postgres'] = True
            logger.info(f"PostgreSQL main database connected successfully: {version}")
            
        except Exception as e:
            connection_status['postgres'] = False
            logger.error(f"Failed to connect to main PostgreSQL database: {e}")
            return connection_status
        
        # Check if required tables exist in postgres
        try:
            conn = self._get_db_connection('postgres')
            cursor = conn.cursor()
            
            # Check if pmn schema exists
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.schemata 
                    WHERE schema_name = 'pmn'
                )
            """)
            
            if not cursor.fetchone()[0]:
                logger.error("Schema 'pmn' does not exist in postgres database")
                connection_status['postgres_schema'] = False
            else:
                connection_status['postgres_schema'] = True
                logger.info("Schema 'pmn' exists in postgres database")
            
            # Check if compiler_status table exists
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'pmn' 
                    AND table_name = 'compiler_status'
                )
            """)
            
            if not cursor.fetchone()[0]:
                logger.error("Table 'pmn.compiler_status' does not exist")
                connection_status['compiler_status_table'] = False
            else:
                connection_status['compiler_status_table'] = True
                logger.info("Table 'pmn.compiler_status' exists")
            
            # Check if compiler_datasets table exists
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'pmn' 
                    AND table_name = 'compiler_datasets'
                )
            """)
            
            if not cursor.fetchone()[0]:
                logger.error("Table 'pmn.compiler_datasets' does not exist")
                connection_status['compiler_datasets_table'] = False
            else:
                connection_status['compiler_datasets_table'] = True
                logger.info("Table 'pmn.compiler_datasets' exists")
            
            cursor.close()
            conn.close()
            
        except Exception as e:
            logger.error(f"Failed to check postgres schema and tables: {e}")
            connection_status['postgres_schema'] = False
            connection_status['compiler_status_table'] = False
            connection_status['compiler_datasets_table'] = False
        
        # Check BPDAS databases
        accessible_bpdas = []
        inaccessible_bpdas = []
        
        for bpdas_db in bpdas_list:
            try:
                conn = self._get_db_connection(bpdas_db)
                cursor = conn.cursor()
                
                # Check if required tables exist
                cursor.execute("""
                    SELECT 
                        EXISTS (
                            SELECT FROM information_schema.tables 
                            WHERE table_schema = 'public' 
                            AND table_name = %s
                        ) as existing_exists,
                        EXISTS (
                            SELECT FROM information_schema.tables 
                            WHERE table_schema = 'public' 
                            AND table_name = %s
                        ) as potensi_exists
                """, (f'existing_{self.year}', f'potensi_{self.year}'))
                
                result = cursor.fetchone()
                existing_exists = result[0]
                potensi_exists = result[1]
                
                cursor.close()
                conn.close()
                
                if existing_exists and potensi_exists:
                    connection_status[bpdas_db] = True
                    accessible_bpdas.append(bpdas_db)
                    logger.info(f"✓ BPDAS '{bpdas_db}': Connected (tables exist)")
                elif existing_exists or potensi_exists:
                    connection_status[bpdas_db] = True
                    accessible_bpdas.append(bpdas_db)
                    logger.warning(f"⚠ BPDAS '{bpdas_db}': Connected (partial tables: existing={existing_exists}, potensi={potensi_exists})")
                else:
                    connection_status[bpdas_db] = False
                    inaccessible_bpdas.append(bpdas_db)
                    logger.warning(f"✗ BPDAS '{bpdas_db}': Connected but tables not found")
                    
            except psycopg2.OperationalError as e:
                connection_status[bpdas_db] = False
                inaccessible_bpdas.append(bpdas_db)
                logger.warning(f"✗ BPDAS '{bpdas_db}': Connection failed - {e}")
            except Exception as e:
                connection_status[bpdas_db] = False
                inaccessible_bpdas.append(bpdas_db)
                logger.warning(f"✗ BPDAS '{bpdas_db}': Unexpected error - {e}")
        
        # Summary
        logger.info("=" * 60)
        logger.info("DATABASE CONNECTION SUMMARY:")
        logger.info(f"  Accessible BPDAS: {len(accessible_bpdas)}/{len(bpdas_list)}")
        logger.info(f"  Inaccessible BPDAS: {len(inaccessible_bpdas)}/{len(bpdas_list)}")
        
        if inaccessible_bpdas:
            logger.warning(f"  Inaccessible databases: {', '.join(inaccessible_bpdas[:5])}" + 
                        (f" and {len(inaccessible_bpdas)-5} more..." if len(inaccessible_bpdas) > 5 else ""))
        
        logger.info("=" * 60)
        
        return connection_status

    def _check_system_requirements(self) -> Dict[str, bool]:
        """Check system requirements (ogr2ogr, tippecanoe)"""
        logger.info("Checking system requirements...")
        
        requirements = {}
        
        # Check ogr2ogr
        try:
            result = subprocess.run(['ogr2ogr', '--version'], 
                                capture_output=True, 
                                text=True, 
                                timeout=5)
            if result.returncode == 0:
                requirements['ogr2ogr'] = True
                logger.info(f"✓ ogr2ogr found: {result.stdout.strip()}")
            else:
                requirements['ogr2ogr'] = False
                logger.error("✗ ogr2ogr not working properly")
        except FileNotFoundError:
            requirements['ogr2ogr'] = False
            logger.error("✗ ogr2ogr not found. Please install GDAL.")
        except Exception as e:
            requirements['ogr2ogr'] = False
            logger.error(f"✗ Error checking ogr2ogr: {e}")
        
        # Check tippecanoe
        try:
            result = subprocess.run(['tippecanoe', '--version'], 
                                capture_output=True, 
                                text=True, 
                                timeout=5)
            if result.returncode == 0:
                requirements['tippecanoe'] = True
                logger.info(f"✓ tippecanoe found: {result.stdout.strip()}")
            else:
                requirements['tippecanoe'] = False
                logger.error("✗ tippecanoe not working properly")
        except FileNotFoundError:
            requirements['tippecanoe'] = False
            logger.error("✗ tippecanoe not found. Please install tippecanoe.")
        except Exception as e:
            requirements['tippecanoe'] = False
            logger.error(f"✗ Error checking tippecanoe: {e}")
        
        # Check disk space in temp directory
        try:
            stat = os.statvfs(tempfile.gettempdir())
            free_space_gb = (stat.f_bavail * stat.f_frsize) / (1024**3)
            
            if free_space_gb < 10:  # Minimum 10GB required
                requirements['disk_space'] = False
                logger.error(f"✗ Insufficient disk space: {free_space_gb:.2f}GB available (minimum 10GB required)")
            else:
                requirements['disk_space'] = True
                logger.info(f"✓ Sufficient disk space: {free_space_gb:.2f}GB available")
                
        except Exception as e:
            requirements['disk_space'] = False
            logger.warning(f"⚠ Could not check disk space: {e}")
        
        return requirements

    def step_0_preflight_checks(self) -> bool:
        """Step 0: Preflight Checks - Verify all prerequisites (5%)"""
        logger.info("=" * 60)
        logger.info("Step 0: Running preflight checks...")
        logger.info("=" * 60)
        
        all_checks_passed = True
        
        try:
            # 1. Check MinIO connection
            if not self._check_minio_connection():
                logger.error("PREFLIGHT FAILED: MinIO connection check failed")
                all_checks_passed = False
            
            # 2. Check system requirements
            system_reqs = self._check_system_requirements()
            if not all(system_reqs.values()):
                logger.error("PREFLIGHT FAILED: System requirements check failed")
                failed_reqs = [k for k, v in system_reqs.items() if not v]
                logger.error(f"Missing requirements: {', '.join(failed_reqs)}")
                all_checks_passed = False
            
            # 3. Get BPDAS list first
            logger.info("Fetching BPDAS list for database checks...")
            bpdas_list = ['agamkuantan', 'akemalamo', 'asahanbarumun', 
            'barito', 'batanghari', 'baturusacerucuk', 'benainnoelmina', 
            'bonelimboto', 'brantassampean', 'cimanukcitanduy', 'citarumciliwung', 
            'dodokanmoyosari', 'indragirirokan', 'jeneberangsaddang', 'kahayan', 'kapuas', 
            'karama', 'ketahun', 'konaweha', 'kruengaceh', 'mahakamberau', 'memberamo', 'musi', 
            'paluposo', 'pemalijratun', 'remuransiki', 'seijangduriangkang', 'serayuopakprogo', 
            'solo', 'tondano', 'undaanyar', 'waehapubatumerah', 'wampuseiular', 'wayseputihwaysekampung']
            
            # 4. Check database connections
            db_status = self._check_database_connections(bpdas_list)
            
            # Check critical postgres components
            critical_postgres_checks = [
                'postgres', 
                'postgres_schema', 
                'compiler_status_table', 
                'compiler_datasets_table'
            ]
            
            for check in critical_postgres_checks:
                if check not in db_status or not db_status[check]:
                    logger.error(f"PREFLIGHT FAILED: Critical postgres check '{check}' failed")
                    all_checks_passed = False
            
            # Count accessible BPDAS
            accessible_bpdas = sum(1 for k, v in db_status.items() 
                                if k not in critical_postgres_checks and v)
            
            if accessible_bpdas == 0:
                logger.error("PREFLIGHT FAILED: No BPDAS databases are accessible")
                all_checks_passed = False
            else:
                logger.info(f"PREFLIGHT: {accessible_bpdas} BPDAS databases are accessible")
            
            # Store accessible BPDAS list for later use
            self.accessible_bpdas = [bpdas for bpdas in bpdas_list if db_status.get(bpdas, False)]
            
            # 5. Update progress
            if all_checks_passed:
                self._update_progress(5, 'PROCESSING')
                logger.info("=" * 60)
                logger.info("✓ ALL PREFLIGHT CHECKS PASSED")
                logger.info("=" * 60)
            else:
                self._update_progress(0, 'PREFLIGHT_FAILED')
                logger.error("=" * 60)
                logger.error("✗ PREFLIGHT CHECKS FAILED")
                logger.error("=" * 60)
            
            return all_checks_passed
            
        except Exception as e:
            logger.error(f"Preflight checks failed with exception: {e}")
            self._update_progress(0, 'PREFLIGHT_FAILED')
            return False

    def step_1_validate_tables(self):
        """Step 1: Validasi Tabel (10%)"""
        logger.info("Step 1: Validating tables...")
        
        try:
            conn = self._get_db_connection()
            cursor = conn.cursor()
            
            # Drop dan recreate tables untuk memastikan struktur yang benar
            tables_config = {
                f'existing_{self.year}_copy1': """
                    CREATE TABLE IF NOT EXISTS pmn.existing_{year}_copy1 (
                        ogc_fid SERIAL PRIMARY KEY,
                        geometry GEOMETRY(MultiPolygon, 4326),
                        bpdas VARCHAR(255),
                        kttj VARCHAR(255),
                        smbdt TEXT,
                        thnbuat VARCHAR(50),
                        ints VARCHAR(255),
                        remark TEXT,
                        struktur_v VARCHAR(255),
                        lsmgr NUMERIC,
                        shape_leng NUMERIC,
                        shape_area NUMERIC,
                        namobj VARCHAR(255),
                        fcode VARCHAR(50),
                        lcode VARCHAR(50),
                        srs_id INTEGER,
                        metadata VARCHAR(255),
                        kode_prov VARCHAR(10),
                        fungsikws VARCHAR(255),
                        noskkws VARCHAR(255),
                        tglskkws TIMESTAMP,
                        lskkws NUMERIC,
                        kawasan VARCHAR(255),
                        konservasi VARCHAR(255),
                        kab VARCHAR(255),
                        prov VARCHAR(255),
                        status_p VARCHAR(255),
                        alasan_p TEXT,
                        kttj_p VARCHAR(255),
                        catatan_p TEXT
                    )
                """,
                f'potensi_{self.year}_copy1': """
                    CREATE TABLE IF NOT EXISTS pmn.potensi_{year}_copy1 (
                        ogc_fid SERIAL PRIMARY KEY,
                        geometry GEOMETRY(MultiPolygon, 4326),
                        tahun VARCHAR(50),
                        objectid INTEGER,
                        bpdas VARCHAR(255),
                        kab VARCHAR(255),
                        prov VARCHAR(255),
                        smbrdt TEXT,
                        thnbuat VARCHAR(50),
                        ints VARCHAR(255),
                        ktrgn TEXT,
                        keterangan TEXT,
                        alasan TEXT,
                        remark TEXT,
                        klshtn VARCHAR(255),
                        kws VARCHAR(255),
                        namobj VARCHAR(255),
                        kawasan VARCHAR(255),
                        lspmgr NUMERIC,
                        status_p VARCHAR(255),
                        alasan_p TEXT,
                        ptrmgr_p VARCHAR(255),
                        catatan_p TEXT
                    )
                """
            }
            
            for table_name, create_sql in tables_config.items():
                cursor.execute(f"""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'pmn' 
                        AND table_name = %s
                    )
                """, (table_name,))
                
                exists = cursor.fetchone()[0]
                
                if exists:
                    logger.info(f"Table exists: pmn.{table_name}")
                else:
                    # Create table with proper structure
                    cursor.execute(create_sql.format(year=self.year))
                    logger.info(f"Created table: pmn.{table_name}")
            
            conn.commit()
            cursor.close()
            conn.close()
            
            self._update_progress(10)
            logger.info("Step 1 completed: Tables validated")
            
        except Exception as e:
            logger.error(f"Step 1 failed: {e}")
            raise

    def step_2_clean_data(self):
        """Step 2: Pembersihan Data (20%)"""
        logger.info("Step 2: Cleaning data...")
        
        try:
            conn = self._get_db_connection()
            cursor = conn.cursor()
            
            # Delete all records from target tables
            tables_to_clean = [
                f'pmn.existing_{self.year}_copy1',
                f'pmn.potensi_{self.year}_copy1'
            ]
            
            for table in tables_to_clean:
                cursor.execute(f"DELETE FROM {table}")
                deleted_count = cursor.rowcount
                logger.info(f"Deleted {deleted_count} records from {table}")
            
            conn.commit()
            cursor.close()
            conn.close()
            
            self._update_progress(20)
            logger.info("Step 2 completed: Data cleaned")
            
        except Exception as e:
            logger.error(f"Step 2 failed: {e}")
            raise

    def step_3_get_bpdas_list(self) -> List[str]:
        """Step 3: Ambil List BPDAS (30%)"""
        logger.info("Step 3: Getting BPDAS list...")
        
        try:
            url = "https://api.ptnaghayasha.com/api/master/cursor/wilayah"
            params = {
                'include_bpdas': 'true',
                'include_provinsi': 'false',
                'include_kabupaten': 'false',
                'include_kecamatan': 'false',
                'include_desa': 'false' 
            }
            
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            bpdas_list = [item['value'].lower() for item in data['data'] if item['type'] == 'bpdas']
            
            logger.info(f"Found {len(bpdas_list)} BPDAS databases: {bpdas_list}")
            
            self._update_progress(30)
            logger.info("Step 3 completed: BPDAS list retrieved")
            
            return bpdas_list
            
        except Exception as e:
            logger.error(f"Step 3 failed: {e}")
            raise

    def step_4_aggregate_data(self, bpdas_list: List[str]):
        """Step 4: Agregasi Data dari Semua BPDAS (40%)"""
        logger.info("Step 4: Aggregating data from all BPDAS...")
        
        try:
            target_conn = self._get_db_connection()
            target_cursor = target_conn.cursor()
            
            for bpdas_db in bpdas_list:
                logger.info(f"Processing BPDAS: {bpdas_db}")
                
                try:
                    # Connect to BPDAS database
                    bpdas_conn = self._get_db_connection(bpdas_db)
                    bpdas_cursor = bpdas_conn.cursor()
                    
                    # ========== PROCESS POTENSI DATA ==========
                    try:
                        # Check if table exists
                        bpdas_cursor.execute("""
                            SELECT EXISTS (
                                SELECT FROM information_schema.tables 
                                WHERE table_schema = 'public' 
                                AND table_name = %s
                            )
                        """, (f'potensi_{self.year}',))
                        
                        if not bpdas_cursor.fetchone()[0]:
                            logger.warning(f"Table potensi_{self.year} not found in {bpdas_db}")
                        else:
                            # Get source table columns
                            bpdas_cursor.execute("""
                                SELECT column_name 
                                FROM information_schema.columns 
                                WHERE table_schema = 'public' AND table_name = %s
                                ORDER BY ordinal_position
                            """, (f'potensi_{self.year}',))
                            source_potensi_cols = [row[0] for row in bpdas_cursor.fetchall()]
                            
                            # Get target table columns
                            target_cursor.execute("""
                                SELECT column_name 
                                FROM information_schema.columns 
                                WHERE table_schema = 'pmn' AND table_name = %s
                                AND column_name != 'ogc_fid'
                                ORDER BY ordinal_position
                            """, (f'potensi_{self.year}_copy1',))
                            target_cols = [row[0] for row in target_cursor.fetchall()]
                            
                            # Find matching columns
                            matching_cols = [col for col in source_potensi_cols if col in target_cols]
                            
                            if not matching_cols:
                                logger.warning(f"No matching columns for potensi_{self.year} in {bpdas_db}")
                            else:
                                logger.info(f"Matching potensi columns ({len(matching_cols)}): {matching_cols[:5]}...")
                                
                                # Build select with geometry conversion
                                select_fields = []
                                for col in matching_cols:
                                    if col.lower() == 'geometry':
                                        select_fields.append("ST_Force2D(ST_Multi(geometry)) AS geometry")
                                    else:
                                        select_fields.append(f'"{col}"')  # Quote column names
                                
                                # Fetch data
                                query = f"""
                                    SELECT {', '.join(select_fields)} 
                                    FROM public.potensi_{self.year}
                                    WHERE geometry IS NOT NULL
                                """
                                bpdas_cursor.execute(query)
                                potensi_data = bpdas_cursor.fetchall()
                                
                                if potensi_data:
                                    # Bulk insert
                                    insert_query = f"""
                                        INSERT INTO pmn.potensi_{self.year}_copy1 
                                        ({', '.join([f'"{col}"' for col in matching_cols])}) 
                                        VALUES ({', '.join(['%s'] * len(matching_cols))})
                                    """
                                    
                                    psycopg2.extras.execute_batch(
                                        target_cursor, 
                                        insert_query, 
                                        potensi_data,
                                        page_size=1000
                                    )
                                    logger.info(f"✓ Inserted {len(potensi_data)} potensi records from {bpdas_db}")
                                else:
                                    logger.info(f"No potensi data in {bpdas_db}")
                                    
                    except psycopg2.Error as e:
                        logger.warning(f"Failed to process potensi from {bpdas_db}: {e}")
                    
                    # ========== PROCESS EXISTING DATA ==========
                    try:
                        # Check if table exists
                        bpdas_cursor.execute("""
                            SELECT EXISTS (
                                SELECT FROM information_schema.tables 
                                WHERE table_schema = 'public' 
                                AND table_name = %s
                            )
                        """, (f'existing_{self.year}',))
                        
                        if not bpdas_cursor.fetchone()[0]:
                            logger.warning(f"Table existing_{self.year} not found in {bpdas_db}")
                        else:
                            # Get source table columns
                            bpdas_cursor.execute("""
                                SELECT column_name 
                                FROM information_schema.columns 
                                WHERE table_schema = 'public' AND table_name = %s
                                ORDER BY ordinal_position
                            """, (f'existing_{self.year}',))
                            source_existing_cols = [row[0] for row in bpdas_cursor.fetchall()]
                            
                            # Get target table columns
                            target_cursor.execute("""
                                SELECT column_name 
                                FROM information_schema.columns 
                                WHERE table_schema = 'pmn' AND table_name = %s
                                AND column_name != 'ogc_fid'
                                ORDER BY ordinal_position
                            """, (f'existing_{self.year}_copy1',))
                            target_cols = [row[0] for row in target_cursor.fetchall()]
                            
                            # Find matching columns
                            matching_cols = [col for col in source_existing_cols if col in target_cols]
                            
                            if not matching_cols:
                                logger.warning(f"No matching columns for existing_{self.year} in {bpdas_db}")
                            else:
                                logger.info(f"Matching existing columns ({len(matching_cols)}): {matching_cols[:5]}...")
                                
                                # Build select with geometry conversion
                                select_fields = []
                                for col in matching_cols:
                                    if col.lower() == 'geometry':
                                        select_fields.append("ST_Force2D(ST_Multi(geometry)) AS geometry")
                                    else:
                                        select_fields.append(f'"{col}"')
                                
                                # Fetch data
                                query = f"""
                                    SELECT {', '.join(select_fields)} 
                                    FROM public.existing_{self.year}
                                    WHERE geometry IS NOT NULL
                                """
                                bpdas_cursor.execute(query)
                                existing_data = bpdas_cursor.fetchall()
                                
                                if existing_data:
                                    # Bulk insert
                                    insert_query = f"""
                                        INSERT INTO pmn.existing_{self.year}_copy1 
                                        ({', '.join([f'"{col}"' for col in matching_cols])}) 
                                        VALUES ({', '.join(['%s'] * len(matching_cols))})
                                    """
                                    
                                    psycopg2.extras.execute_batch(
                                        target_cursor, 
                                        insert_query, 
                                        existing_data,
                                        page_size=1000
                                    )
                                    logger.info(f"✓ Inserted {len(existing_data)} existing records from {bpdas_db}")
                                else:
                                    logger.info(f"No existing data in {bpdas_db}")
                                    
                    except psycopg2.Error as e:
                        logger.warning(f"Failed to process existing from {bpdas_db}: {e}")
                    
                    # Commit after each BPDAS
                    target_conn.commit()
                    
                    bpdas_cursor.close()
                    bpdas_conn.close()
                    
                    logger.info(f"✓ Completed BPDAS: {bpdas_db}")
                    
                except psycopg2.OperationalError as e:
                    logger.warning(f"✗ Cannot connect to {bpdas_db}: {e}")
                    continue
                except Exception as e:
                    logger.warning(f"✗ Error processing {bpdas_db}: {e}")
                    continue
            
            # Final commit
            target_conn.commit()
            
            # Log summary
            target_cursor.execute(f"SELECT COUNT(*) FROM pmn.existing_{self.year}_copy1")
            existing_count = target_cursor.fetchone()[0]
            
            target_cursor.execute(f"SELECT COUNT(*) FROM pmn.potensi_{self.year}_copy1")
            potensi_count = target_cursor.fetchone()[0]
            
            logger.info("=" * 60)
            logger.info("AGGREGATION SUMMARY:")
            logger.info(f"  Total existing records: {existing_count}")
            logger.info(f"  Total potensi records: {potensi_count}")
            logger.info("=" * 60)
            
            target_cursor.close()
            target_conn.close()
            
            self._update_progress(40)
            logger.info("Step 4 completed: Data aggregation finished")
        
        except Exception as e:
            logger.error(f"Step 4 failed: {e}")
            raise
    
    def step_5_clean_minio_files(self):
        """Step 5: Bersihkan File Lama di MinIO (50%)"""
        logger.info("Step 5: Cleaning old MinIO files...")
        
        try:
            # List and delete files in pmn-result/{year}/ path
            prefix = f"pmn-result/{self.year}/"
            
            objects = self.minio_client.list_objects(
                MINIO_CONFIG['bucket'], 
                prefix=prefix, 
                recursive=True
            )
            
            deleted_count = 0
            for obj in objects:
                self.minio_client.remove_object(MINIO_CONFIG['bucket'], obj.object_name)
                deleted_count += 1
                logger.info(f"Deleted: {obj.object_name}")
            
            logger.info(f"Deleted {deleted_count} files from MinIO")
            
            self._update_progress(50)
            logger.info("Step 5 completed: MinIO files cleaned")
            
        except Exception as e:
            logger.error(f"Step 5 failed: {e}")
            raise

    def step_6_delete_old_pmtiles(self):
        """Step 6: Hapus PMTiles Lama (60%)"""
        logger.info("Step 6: Deleting old PMTiles...")
        
        try:
            pmtiles_files = [
                f"layers/EXISTING{self.year}.pmtiles",
                f"layers/POTENSI{self.year}.pmtiles"
            ]
            
            for pmtiles_file in pmtiles_files:
                try:
                    self.minio_client.remove_object(MINIO_CONFIG['bucket'], pmtiles_file)
                    logger.info(f"Deleted PMTiles: {pmtiles_file}")
                except S3Error as e:
                    if e.code == 'NoSuchKey':
                        logger.info(f"PMTiles file not found (already deleted): {pmtiles_file}")
                    else:
                        raise
            
            self._update_progress(60)
            logger.info("Step 6 completed: Old PMTiles deleted")
            
        except Exception as e:
            logger.error(f"Step 6 failed: {e}")
            raise

    def step_7_export_geojson(self) -> Dict[str, str]:
        """Step 7: Export ke GeoJSON (70%)"""
        logger.info("Step 7: Exporting to GeoJSON...")
        
        try:
            conn = self._get_db_connection()
            
            geojson_files = {}
            
            # Export existing data
            logger.info("Exporting existing data...")
            existing_gdf = gpd.read_postgis(
                f"SELECT * FROM pmn.existing_{self.year}_copy1",
                conn,
                geom_col='geometry'
            )
            
            # Log data info
            logger.info(f"Existing data shape: {existing_gdf.shape}")
            logger.info(f"Existing columns: {list(existing_gdf.columns)}")
            logger.info("Existing data types:")
            for col, dtype in existing_gdf.dtypes.items():
                if col != 'geometry':
                    logger.info(f"  {col}: {dtype}")
            
            existing_file = os.path.join(self.temp_dir, f'existing_{self.year}.geojson')
            existing_gdf.to_file(existing_file, driver='GeoJSON')
            geojson_files['existing'] = existing_file
            logger.info(f"✓ Exported existing: {len(existing_gdf)} features, {os.path.getsize(existing_file):,} bytes")
            
            # Export potensi data
            logger.info("Exporting potensi data...")
            potensi_gdf = gpd.read_postgis(
                f"SELECT * FROM pmn.potensi_{self.year}_copy1",
                conn,
                geom_col='geometry'
            )
            
            # Log data info
            logger.info(f"Potensi data shape: {potensi_gdf.shape}")
            logger.info(f"Potensi columns: {list(potensi_gdf.columns)}")
            logger.info("Potensi data types:")
            for col, dtype in potensi_gdf.dtypes.items():
                if col != 'geometry':
                    logger.info(f"  {col}: {dtype}")
            
            potensi_file = os.path.join(self.temp_dir, f'potensi_{self.year}.geojson')
            potensi_gdf.to_file(potensi_file, driver='GeoJSON')
            geojson_files['potensi'] = potensi_file
            logger.info(f"✓ Exported potensi: {len(potensi_gdf)} features, {os.path.getsize(potensi_file):,} bytes")
            
            conn.close()
            
            self._update_progress(70)
            logger.info("Step 7 completed: GeoJSON export finished")
            
            return geojson_files
            
        except Exception as e:
            logger.error(f"Step 7 failed: {e}")
            import traceback
            logger.error(traceback.format_exc())
            raise
    
    def step_8_convert_formats(self, geojson_files: Dict[str, str]) -> Dict[str, Dict[str, str]]:
        """Step 8: Konversi Format (80%)"""
        logger.info("Step 8: Converting formats...")
        
        try:
            converted_files = {}
            
            for theme, geojson_file in geojson_files.items():
                theme_files = {}
                
                # Read GeoJSON
                gdf = gpd.read_file(geojson_file)
                logger.info(f"Read {theme} GeoJSON: {len(gdf)} features")
                logger.info(f"Original columns ({len(gdf.columns)}): {list(gdf.columns)}")
                logger.info(f"Data types:\n{gdf.dtypes}")
                
                # ========== PREPARE FOR SHAPEFILE ==========
                # Create a copy for Shapefile (needs special handling)
                gdf_shp = gdf.copy()
                
                # 1. Convert datetime columns to string
                datetime_cols = []
                for col in gdf_shp.columns:
                    if col == 'geometry':
                        continue
                    
                    # Check for datetime types
                    if pd.api.types.is_datetime64_any_dtype(gdf_shp[col]):
                        datetime_cols.append(col)
                        logger.info(f"Converting datetime column '{col}' to string")
                        # Convert to string, handle NaT
                        gdf_shp[col] = gdf_shp[col].apply(
                            lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notna(x) else ''
                        )
                
                logger.info(f"Converted {len(datetime_cols)} datetime columns: {datetime_cols}")
                
                # 2. Handle other problematic data types
                for col in gdf_shp.columns:
                    if col == 'geometry':
                        continue
                    
                    # Convert object types that might contain datetime
                    if gdf_shp[col].dtype == 'object':
                        # Check if it's a datetime string
                        sample = gdf_shp[col].dropna().head(1)
                        if len(sample) > 0:
                            try:
                                pd.to_datetime(sample.iloc[0])
                                logger.info(f"Column '{col}' contains datetime strings, keeping as string")
                                gdf_shp[col] = gdf_shp[col].fillna('').astype(str)
                            except:
                                # Not datetime, keep as is but ensure no None values
                                gdf_shp[col] = gdf_shp[col].fillna('')
                
                # 3. Truncate column names to 10 characters (Shapefile limitation)
                column_mapping = {}
                used_names = set()
                
                for col in gdf_shp.columns:
                    if col == 'geometry':
                        continue
                        
                    if len(col) > 10:
                        # Create short name
                        base_name = col[:10]
                        new_col = base_name
                        
                        # Ensure uniqueness
                        counter = 1
                        while new_col in used_names or new_col in gdf_shp.columns:
                            new_col = col[:8] + f"{counter:02d}"
                            counter += 1
                            if counter > 99:  # Safety limit
                                new_col = col[:7] + f"{counter:03d}"
                        
                        column_mapping[col] = new_col
                        used_names.add(new_col)
                        logger.info(f"Truncating column: '{col}' -> '{new_col}'")
                    else:
                        used_names.add(col)
                
                if column_mapping:
                    gdf_shp = gdf_shp.rename(columns=column_mapping)
                    logger.info(f"Renamed {len(column_mapping)} columns for Shapefile")
                
                # 4. Final data type check
                logger.info("Final Shapefile data types:")
                for col in gdf_shp.columns:
                    if col != 'geometry':
                        logger.info(f"  {col}: {gdf_shp[col].dtype}")
                
                # ========== CREATE SHAPEFILE ==========
                shp_dir = os.path.join(self.temp_dir, f'{theme}_shp')
                os.makedirs(shp_dir, exist_ok=True)
                shp_file = os.path.join(shp_dir, f'AR_25K_PETAMANGROVE_{theme.upper()}_{self.year}.shp')
                
                try:
                    logger.info(f"Creating Shapefile for {theme}...")
                    gdf_shp.to_file(shp_file, driver='ESRI Shapefile', encoding='utf-8')
                    logger.info(f"✓ Shapefile created successfully: {shp_file}")
                except Exception as e:
                    logger.error(f"Failed to create Shapefile: {e}")
                    logger.info("Attempting additional data cleaning...")
                    
                    # Last resort: convert everything to string except geometry and numeric
                    for col in gdf_shp.columns:
                        if col == 'geometry':
                            continue
                        
                        if gdf_shp[col].dtype not in ['int64', 'float64', 'bool', 'Int64', 'Float64']:
                            logger.info(f"Converting '{col}' to string as fallback")
                            gdf_shp[col] = gdf_shp[col].astype(str).replace('NaT', '').replace('nan', '').replace('None', '')
                    
                    gdf_shp.to_file(shp_file, driver='ESRI Shapefile', encoding='utf-8')
                    logger.info(f"✓ Shapefile created after additional cleaning")
                
                # ========== ZIP SHAPEFILE ==========
                shp_zip = os.path.join(self.temp_dir, f'AR_25K_PETAMANGROVE_{theme.upper()}_{self.year}.zip')
                
                with zipfile.ZipFile(shp_zip, 'w', zipfile.ZIP_DEFLATED) as zipf:
                    extensions = ['.shp', '.shx', '.dbf', '.prj', '.cpg']
                    for ext in extensions:
                        file_path = shp_file.replace('.shp', ext)
                        if os.path.exists(file_path):
                            zipf.write(file_path, os.path.basename(file_path))
                            logger.info(f"  Added {ext} to zip")
                
                shp_size = os.path.getsize(shp_zip)
                theme_files['shp_zip'] = shp_zip
                logger.info(f"✓ Shapefile zip created: {shp_zip} ({shp_size:,} bytes)")
                
                # ========== CREATE GDB (using original geojson - supports datetime) ==========
                gdb_dir = os.path.join(self.temp_dir, f'AR_25K_PETAMANGROVE_{theme.upper()}_{self.year}.gdb')
                
                try:
                    logger.info(f"Creating GDB for {theme}...")
                    
                    # Read GeoJSON and remove ogc_fid to avoid duplicate ID issues
                    gdf_for_gdb = gpd.read_file(geojson_file)
                    
                    # Remove ogc_fid or fid columns if they exist
                    fid_columns = [col for col in gdf_for_gdb.columns if col.lower() in ['ogc_fid', 'fid', 'id', 'objectid']]
                    if fid_columns:
                        logger.info(f"Removing FID columns for GDB: {fid_columns}")
                        gdf_for_gdb = gdf_for_gdb.drop(columns=fid_columns)
                    
                    # Convert datetime columns to string for Shapefile compatibility
                    datetime_cols_gdb = []
                    for col in gdf_for_gdb.columns:
                        if col == 'geometry':
                            continue
                        if pd.api.types.is_datetime64_any_dtype(gdf_for_gdb[col]):
                            datetime_cols_gdb.append(col)
                            logger.info(f"Converting datetime column '{col}' to string for temp Shapefile")
                            gdf_for_gdb[col] = gdf_for_gdb[col].apply(
                                lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notna(x) else ''
                            )
                    
                    # Reset index to ensure clean sequential numbering
                    gdf_for_gdb = gdf_for_gdb.reset_index(drop=True)
                    
                    # Create temporary Shapefile (which doesn't have ID issues) then convert to GDB
                    temp_shp_dir = os.path.join(self.temp_dir, f'{theme}_temp_for_gdb')
                    os.makedirs(temp_shp_dir, exist_ok=True)
                    temp_shp = os.path.join(temp_shp_dir, f'{theme}_temp.shp')
                    
                    # Save to Shapefile first (Shapefile will auto-generate sequential FIDs)
                    gdf_for_gdb.to_file(temp_shp, driver='ESRI Shapefile')
                    logger.info(f"Created temporary Shapefile for GDB conversion")
                    
                    # Convert Shapefile to GDB (this avoids GeoJSON ID issues)
                    result = subprocess.run([
                        'ogr2ogr',
                        '-f', 'OpenFileGDB',
                        '-dim', 'XY',  # Force 2D
                        '-nln', f'PETAMANGROVE_{theme.upper()}_{self.year}',  # Layer name
                        gdb_dir,
                        temp_shp
                    ], check=True, capture_output=True, text=True)
                    
                    # Clean up temporary shapefile directory
                    import shutil
                    if os.path.exists(temp_shp_dir):
                        shutil.rmtree(temp_shp_dir)
                    
                    logger.info(f"✓ GDB created successfully: {gdb_dir}")
                    
                except subprocess.CalledProcessError as e:
                    logger.error(f"ogr2ogr failed for {theme}:")
                    logger.error(f"  stdout: {e.stdout}")
                    logger.error(f"  stderr: {e.stderr}")
                    raise
                
                # ========== ZIP GDB ==========
                gdb_zip = os.path.join(self.temp_dir, f'AR_25K_PETAMANGROVE_{theme.upper()}_{self.year}.gdb.zip')
                
                with zipfile.ZipFile(gdb_zip, 'w', zipfile.ZIP_DEFLATED) as zipf:
                    for root, dirs, files in os.walk(gdb_dir):
                        for file in files:
                            file_path = os.path.join(root, file)
                            arcname = os.path.relpath(file_path, os.path.dirname(gdb_dir))
                            zipf.write(file_path, arcname)
                
                gdb_size = os.path.getsize(gdb_zip)
                theme_files['gdb_zip'] = gdb_zip
                theme_files['gdb_dir'] = gdb_dir
                logger.info(f"✓ GDB zip created: {gdb_zip} ({gdb_size:,} bytes)")
                
                converted_files[theme] = theme_files
                logger.info(f"✓ Completed conversion for {theme}")
                logger.info("=" * 60)
            
            self._update_progress(80)
            logger.info("Step 8 completed: Format conversion finished")
            
            return converted_files
            
        except Exception as e:
            logger.error(f"Step 8 failed: {e}")
            import traceback
            logger.error(traceback.format_exc())
            raise
    
    def step_9_generate_pmtiles(self, geojson_files: Dict[str, str]) -> Dict[str, str]:
        """Step 9: Generate PMTiles (90%)"""
        logger.info("Step 9: Generating PMTiles...")
        
        try:
            pmtiles_urls = {}
            
            for theme, geojson_file in geojson_files.items():
                # Generate PMTiles using tippecanoe
                pmtiles_file = os.path.join(self.temp_dir, f'{theme.upper()}{self.year}.pmtiles')
                
                subprocess.run([
                    'tippecanoe',
                    '-o', pmtiles_file,
                    '--force',
                    '--maximum-zoom=14',
                    '--minimum-zoom=0',
                    geojson_file
                ], check=True)
                
                # Upload to MinIO
                minio_path = f"layers/{theme.upper()}{self.year}.pmtiles"
                self.minio_client.fput_object(
                    MINIO_CONFIG['bucket'],
                    minio_path,
                    pmtiles_file
                )
                
                pmtiles_url = f"{MINIO_CONFIG['public_host']}/{MINIO_CONFIG['bucket']}/{minio_path}"
                pmtiles_urls[theme] = pmtiles_url
                
                logger.info(f"Generated and uploaded PMTiles for {theme}: {pmtiles_url}")
            
            self._update_progress(90)
            logger.info("Step 9 completed: PMTiles generation finished")
            
            return pmtiles_urls
            
        except Exception as e:
            logger.error(f"Step 9 failed: {e}")
            raise

    def step_10_update_metadata(self, converted_files: Dict[str, Dict[str, str]], pmtiles_urls: Dict[str, str]):
        """Step 10: Update Metadata (95%)"""
        logger.info("Step 10: Updating metadata...")
        
        try:
            conn = self._get_db_connection()
            cursor = conn.cursor()
            
            for theme in ['existing', 'potensi']:
                # Get row count
                cursor.execute(f"SELECT COUNT(*) FROM pmn.{theme}_{self.year}")
                row_count = cursor.fetchone()[0]
                
                # Calculate MD5 hash of GDB file
                gdb_file = converted_files[theme]['gdb_zip']
                with open(gdb_file, 'rb') as f:
                    md5_hash = hashlib.md5(f.read()).hexdigest()
                
                # Get file size
                file_size = os.path.getsize(gdb_file)
                
                # Upload files to MinIO
                gdb_minio_path = f"pmn-result/{self.year}/AR_25K_PETAMANGROVE_{theme.upper()}_{self.year}.gdb.zip"
                shp_minio_path = f"pmn-result/{self.year}/AR_25K_PETAMANGROVE_{theme.upper()}_{self.year}.zip"
                
                self.minio_client.fput_object(
                    MINIO_CONFIG['bucket'],
                    gdb_minio_path,
                    converted_files[theme]['gdb_zip']
                )
                
                self.minio_client.fput_object(
                    MINIO_CONFIG['bucket'],
                    shp_minio_path,
                    converted_files[theme]['shp_zip']
                )
                
                # Construct URLs
                gdb_url = f"http://52.76.171.132:9008/{MINIO_CONFIG['bucket']}/{gdb_minio_path}"
                shp_url = f"http://52.76.171.132:9008/{MINIO_CONFIG['bucket']}/{shp_minio_path}"
                
                # Update metadata
                title = f"Peta {'Eksisting' if theme == 'existing' else 'Potensi'} Mangrove {self.year}"
                
                cursor.execute("""
                    UPDATE pmn.compiler_datasets 
                    SET rows = %s, md5 = %s, date_created = %s, size = %s,
                        gdb_url = %s, shp_url = %s, map_url = %s, process = %s
                    WHERE title = %s
                """, (
                    row_count, md5_hash, datetime.now(), file_size,
                    gdb_url, shp_url, pmtiles_urls[theme], False, title
                ))
                
                logger.info(f"Updated metadata for {theme}: {row_count} rows, {file_size} bytes")
            
            conn.commit()
            cursor.close()
            conn.close()
            
            self._update_progress(95)
            logger.info("Step 10 completed: Metadata updated")
            
        except Exception as e:
            logger.error(f"Step 10 failed: {e}")
            raise

    def step_11_finalize(self):
        """Step 11: Finalisasi (100%)"""
        logger.info("Step 11: Finalizing...")
        
        try:
            self._update_progress(100, 'COMPLETED')
            logger.info("Step 11 completed: Process finalized")
            
        except Exception as e:
            logger.error(f"Step 11 failed: {e}")
            raise

    def cleanup(self):
        """Clean up temporary files"""
        try:
            if self.temp_dir and os.path.exists(self.temp_dir):
                import shutil
                shutil.rmtree(self.temp_dir)
                logger.info(f"Cleaned up temporary directory: {self.temp_dir}")
        except Exception as e:
            logger.warning(f"Failed to cleanup temporary directory: {e}")

    def _check_file_exists_in_minio(self, object_path: str) -> bool:
        """Check if file exists in MinIO"""
        try:
            self.minio_client.stat_object(MINIO_CONFIG['bucket'], object_path)
            return True
        except S3Error:
            return False

    def _check_year_files_exist(self, year: int) -> Dict[str, bool]:
        """Check which files exist for a specific year"""
        files_status = {}
        
        # Check PMTiles
        pmtiles_existing = f"layers/EXISTING{year}.pmtiles"
        pmtiles_potensi = f"layers/POTENSI{year}.pmtiles"
        files_status['pmtiles_existing'] = self._check_file_exists_in_minio(pmtiles_existing)
        files_status['pmtiles_potensi'] = self._check_file_exists_in_minio(pmtiles_potensi)
        
        # Check Shapefile
        shp_existing = f"pmn-result/{year}/AR_25K_PETAMANGROVE_EXISTING_{year}.zip"
        shp_potensi = f"pmn-result/{year}/AR_25K_PETAMANGROVE_POTENSI_{year}.zip"
        files_status['shp_existing'] = self._check_file_exists_in_minio(shp_existing)
        files_status['shp_potensi'] = self._check_file_exists_in_minio(shp_potensi)
        
        # Check GDB
        gdb_existing = f"pmn-result/{year}/AR_25K_PETAMANGROVE_EXISTING_{year}.gdb.zip"
        gdb_potensi = f"pmn-result/{year}/AR_25K_PETAMANGROVE_POTENSI_{year}.gdb.zip"
        files_status['gdb_existing'] = self._check_file_exists_in_minio(gdb_existing)
        files_status['gdb_potensi'] = self._check_file_exists_in_minio(gdb_potensi)
        
        return files_status

    def _convert_pmtiles_to_formats(self, year: int):
        """Convert PMTiles to Shapefile and GDB for specific year"""
        logger.info(f"Converting PMTiles to Shapefile and GDB for year {year}")
        
        # Check if both PMTiles exist
        pmtiles_existing = f"layers/EXISTING{year}.pmtiles"
        pmtiles_potensi = f"layers/POTENSI{year}.pmtiles"
        
        if not (self._check_file_exists_in_minio(pmtiles_existing) and 
                self._check_file_exists_in_minio(pmtiles_potensi)):
            logger.warning(f"PMTiles files not found for year {year}, skipping conversion")
            return
        
        # Download PMTiles from MinIO
        temp_pmtiles_dir = os.path.join(self.temp_dir, f'pmtiles_{year}')
        os.makedirs(temp_pmtiles_dir, exist_ok=True)
        
        try:
            # Download existing PMTiles
            existing_pmtiles = os.path.join(temp_pmtiles_dir, f'existing_{year}.pmtiles')
            self.minio_client.fget_object(MINIO_CONFIG['bucket'], pmtiles_existing, existing_pmtiles)
            
            # Download potensi PMTiles
            potensi_pmtiles = os.path.join(temp_pmtiles_dir, f'potensi_{year}.pmtiles')
            self.minio_client.fget_object(MINIO_CONFIG['bucket'], pmtiles_potensi, potensi_pmtiles)
            
            # Convert PMTiles to GeoJSON first
            for theme, pmtiles_file in [('existing', existing_pmtiles), ('potensi', potensi_pmtiles)]:
                logger.info(f"Converting {theme} PMTiles to GeoJSON for year {year}")
                
                # Convert PMTiles to GeoJSON using tippecanoe-decode
                geojson_file = os.path.join(temp_pmtiles_dir, f'{theme}_{year}.geojson')
                
                # Use multiple methods to convert PMTiles to GeoJSON
                conversion_success = False
                
                # Method 1: Try pmtiles extract first (if available)
                try:
                    logger.info(f"Trying pmtiles extract for {theme}...")
                    result = subprocess.run([
                        'pmtiles', 'extract', pmtiles_file, geojson_file
                    ], check=True, capture_output=True, text=True, timeout=300)
                    
                    if os.path.exists(geojson_file) and os.path.getsize(geojson_file) > 0:
                        logger.info(f"✅ pmtiles extract successful")
                        conversion_success = True
                except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired) as e:
                    logger.warning(f"pmtiles extract failed: {e}")
                
                # Method 2: Try tippecanoe-decode with warning tolerance
                if not conversion_success:
                    try:
                        logger.info(f"Trying tippecanoe-decode for {theme}...")
                        result = subprocess.run([
                            'tippecanoe-decode', pmtiles_file
                        ], capture_output=True, text=True, timeout=300)
                        
                        # tippecanoe-decode may return exit code 106 for geometry warnings
                        # but still produce valid output
                        if result.stdout and len(result.stdout) > 100:
                            with open(geojson_file, 'w') as f:
                                f.write(result.stdout)
                            
                            if os.path.exists(geojson_file) and os.path.getsize(geojson_file) > 0:
                                logger.info(f"✅ tippecanoe-decode successful (exit code: {result.returncode})")
                                if result.returncode != 0:
                                    logger.warning(f"tippecanoe-decode warnings: {result.stderr}")
                                conversion_success = True
                        else:
                            logger.error(f"tippecanoe-decode produced no output")
                            
                    except subprocess.TimeoutExpired:
                        logger.error(f"tippecanoe-decode timeout")
                    except Exception as e:
                        logger.error(f"tippecanoe-decode error: {e}")
                
                # Method 3: Try ogr2ogr direct conversion (if PMTiles driver available)
                if not conversion_success:
                    try:
                        logger.info(f"Trying ogr2ogr direct conversion for {theme}...")
                        result = subprocess.run([
                            'ogr2ogr', '-f', 'GeoJSON', geojson_file, pmtiles_file
                        ], check=True, capture_output=True, text=True, timeout=300)
                        
                        if os.path.exists(geojson_file) and os.path.getsize(geojson_file) > 0:
                            logger.info(f"✅ ogr2ogr direct conversion successful")
                            conversion_success = True
                    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
                        logger.warning(f"ogr2ogr direct conversion failed: {e}")
                
                # Check if any conversion method succeeded
                if not conversion_success:
                    raise Exception(f"All PMTiles conversion methods failed for {theme}")
                
                # Verify output file
                if not os.path.exists(geojson_file) or os.path.getsize(geojson_file) == 0:
                    raise Exception(f"No valid GeoJSON output produced for {theme}")
                
                logger.info(f"✓ Successfully converted PMTiles to GeoJSON: {geojson_file} ({os.path.getsize(geojson_file):,} bytes)")
                
                logger.info(f"✓ Converted PMTiles to GeoJSON: {geojson_file}")
                
                # Now convert GeoJSON to Shapefile and GDB
                self._convert_geojson_to_formats(geojson_file, theme, year)
                
        except Exception as e:
            logger.error(f"Failed to convert PMTiles for year {year}: {e}")
            raise
        finally:
            # Cleanup temp PMTiles directory
            if os.path.exists(temp_pmtiles_dir):
                import shutil
                shutil.rmtree(temp_pmtiles_dir)

    def _convert_geojson_to_formats(self, geojson_file: str, theme: str, year: int):
        """Convert GeoJSON to Shapefile and GDB formats"""
        logger.info(f"Converting {theme} GeoJSON to Shapefile and GDB for year {year}")
        
        # Read GeoJSON
        gdf = gpd.read_file(geojson_file)
        logger.info(f"Read {theme} GeoJSON: {len(gdf)} features for year {year}")
        
        # Create year-specific temp directory
        year_temp_dir = os.path.join(self.temp_dir, f'convert_{year}_{theme}')
        os.makedirs(year_temp_dir, exist_ok=True)
        
        try:
            # ========== CREATE SHAPEFILE ==========
            # Prepare data for Shapefile (same logic as main process)
            gdf_shp = gdf.copy()
            
            # Convert datetime columns to string
            for col in gdf_shp.columns:
                if col == 'geometry':
                    continue
                if pd.api.types.is_datetime64_any_dtype(gdf_shp[col]):
                    gdf_shp[col] = gdf_shp[col].apply(
                        lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notna(x) else ''
                    )
            
            # Truncate column names for Shapefile
            column_mapping = {}
            used_names = set()
            for col in gdf_shp.columns:
                if col == 'geometry':
                    continue
                if len(col) > 10:
                    base_name = col[:10]
                    new_col = base_name
                    counter = 1
                    while new_col in used_names or new_col in gdf_shp.columns:
                        new_col = col[:8] + f"{counter:02d}"
                        counter += 1
                        if counter > 99:
                            new_col = col[:7] + f"{counter:03d}"
                    column_mapping[col] = new_col
                    used_names.add(new_col)
                else:
                    used_names.add(col)
            
            if column_mapping:
                gdf_shp = gdf_shp.rename(columns=column_mapping)
            
            # Create Shapefile
            shp_dir = os.path.join(year_temp_dir, f'{theme}_shp')
            os.makedirs(shp_dir, exist_ok=True)
            shp_file = os.path.join(shp_dir, f'AR_25K_PETAMANGROVE_{theme.upper()}_{year}.shp')
            
            gdf_shp.to_file(shp_file, driver='ESRI Shapefile', encoding='utf-8')
            
            # ZIP Shapefile
            shp_zip = os.path.join(year_temp_dir, f'AR_25K_PETAMANGROVE_{theme.upper()}_{year}.zip')
            with zipfile.ZipFile(shp_zip, 'w', zipfile.ZIP_DEFLATED) as zipf:
                extensions = ['.shp', '.shx', '.dbf', '.prj', '.cpg']
                for ext in extensions:
                    file_path = shp_file.replace('.shp', ext)
                    if os.path.exists(file_path):
                        zipf.write(file_path, os.path.basename(file_path))
            
            # Upload Shapefile to MinIO
            shp_minio_path = f"pmn-result/{year}/AR_25K_PETAMANGROVE_{theme.upper()}_{year}.zip"
            self.minio_client.fput_object(MINIO_CONFIG['bucket'], shp_minio_path, shp_zip)
            logger.info(f"✓ Uploaded Shapefile: {shp_minio_path}")
            
            # ========== CREATE GDB ==========
            # Prepare data for GDB (remove FID columns)
            gdf_gdb = gdf.copy()
            fid_columns = [col for col in gdf_gdb.columns if col.lower() in ['ogc_fid', 'fid', 'id', 'objectid']]
            if fid_columns:
                gdf_gdb = gdf_gdb.drop(columns=fid_columns)
            
            # Convert datetime to string for temp Shapefile
            for col in gdf_gdb.columns:
                if col == 'geometry':
                    continue
                if pd.api.types.is_datetime64_any_dtype(gdf_gdb[col]):
                    gdf_gdb[col] = gdf_gdb[col].apply(
                        lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notna(x) else ''
                    )
            
            gdf_gdb = gdf_gdb.reset_index(drop=True)
            
            # Create temp Shapefile for GDB conversion
            temp_shp_dir = os.path.join(year_temp_dir, f'{theme}_temp_for_gdb')
            os.makedirs(temp_shp_dir, exist_ok=True)
            temp_shp = os.path.join(temp_shp_dir, f'{theme}_temp.shp')
            
            gdf_gdb.to_file(temp_shp, driver='ESRI Shapefile')
            
            # Convert to GDB
            gdb_dir = os.path.join(year_temp_dir, f'AR_25K_PETAMANGROVE_{theme.upper()}_{year}.gdb')
            result = subprocess.run([
                'ogr2ogr',
                '-f', 'OpenFileGDB',
                '-dim', 'XY',
                '-nln', f'PETAMANGROVE_{theme.upper()}_{year}',
                gdb_dir,
                temp_shp
            ], check=True, capture_output=True, text=True)
            
            # ZIP GDB
            gdb_zip = os.path.join(year_temp_dir, f'AR_25K_PETAMANGROVE_{theme.upper()}_{year}.gdb.zip')
            with zipfile.ZipFile(gdb_zip, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for root, dirs, files in os.walk(gdb_dir):
                    for file in files:
                        file_path = os.path.join(root, file)
                        arcname = os.path.relpath(file_path, os.path.dirname(gdb_dir))
                        zipf.write(file_path, arcname)
            
            # Upload GDB to MinIO
            gdb_minio_path = f"pmn-result/{year}/AR_25K_PETAMANGROVE_{theme.upper()}_{year}.gdb.zip"
            self.minio_client.fput_object(MINIO_CONFIG['bucket'], gdb_minio_path, gdb_zip)
            logger.info(f"✓ Uploaded GDB: {gdb_minio_path}")
            
        except Exception as e:
            logger.error(f"Failed to convert {theme} for year {year}: {e}")
            raise
        finally:
            # Cleanup
            if os.path.exists(year_temp_dir):
                import shutil
                shutil.rmtree(year_temp_dir)

    def process_historical_years(self):
        """Process historical years (2021 to current_year-1)"""
        current_year = datetime.now().year
        
        logger.info("=" * 60)
        logger.info("PROCESSING HISTORICAL YEARS")
        logger.info("=" * 60)
        
        for year in range(2021, current_year):
            if year == self.year:  # Skip current processing year
                continue
                
            logger.info(f"Checking files for year {year}...")
            files_status = self._check_year_files_exist(year)
            
            # Check if all files exist
            all_files_exist = (
                files_status['pmtiles_existing'] and files_status['pmtiles_potensi'] and
                files_status['shp_existing'] and files_status['shp_potensi'] and
                files_status['gdb_existing'] and files_status['gdb_potensi']
            )
            
            if all_files_exist:
                logger.info(f"✓ All files exist for year {year}, skipping...")
                continue
            
            # Check if only PMTiles exist
            pmtiles_exist = files_status['pmtiles_existing'] and files_status['pmtiles_potensi']
            shp_gdb_missing = not (
                files_status['shp_existing'] and files_status['shp_potensi'] and
                files_status['gdb_existing'] and files_status['gdb_potensi']
            )
            
            if pmtiles_exist and shp_gdb_missing:
                logger.info(f"📁 PMTiles exist but Shapefile/GDB missing for year {year}, converting...")
                try:
                    self._convert_pmtiles_to_formats(year)
                    logger.info(f"✓ Successfully converted files for year {year}")
                except Exception as e:
                    logger.error(f"❌ Failed to convert files for year {year}: {e}")
                    continue
            else:
                logger.warning(f"⚠️  Year {year} requires full processing (missing PMTiles), skipping for now...")
                logger.info(f"  Files status: {files_status}")
        
        logger.info("Historical years processing completed")
        logger.info("=" * 60)

    def run(self):
        """Run the complete PMN compilation process"""
        try:
            logger.info(f"Starting PMN compilation for year {self.year}, process ID: {self.process_id}")

            # Step 0: Preflight checks
            if not self.step_0_preflight_checks():
                logger.error("Preflight checks failed. Aborting compilation.")
                return
            
            # Step 1: Validate tables
            self.step_1_validate_tables()
            
            # Step 2: Clean data
            self.step_2_clean_data()
            
            # Step 3: Get BPDAS list
            bpdas_list = ['agamkuantan', 'akemalamo', 'asahanbarumun', 
            'barito', 'batanghari', 'baturusacerucuk', 'benainnoelmina', 
            'bonelimboto', 'brantassampean', 'cimanukcitanduy', 'citarumciliwung', 
            'dodokanmoyosari', 'indragirirokan', 'jeneberangsaddang', 'kahayan', 'kapuas', 
            'karama', 'ketahun', 'konaweha', 'kruengaceh', 'mahakamberau', 'memberamo', 'musi', 
            'paluposo', 'pemalijratun', 'remuransiki', 'seijangduriangkang', 'serayuopakprogo', 
            'solo', 'tondano', 'undaanyar', 'waehapubatumerah', 'wampuseiular', 'wayseputihwaysekampung']
            
            # Step 4: Aggregate data
            self.step_4_aggregate_data(bpdas_list)
            
            # Step 5: Clean MinIO files
            self.step_5_clean_minio_files()
            
            # Step 6: Delete old PMTiles
            self.step_6_delete_old_pmtiles()
            
            # Step 7: Export to GeoJSON
            geojson_files = self.step_7_export_geojson()
            
            # Step 8: Convert formats
            converted_files = self.step_8_convert_formats(geojson_files)
            
            # Step 9: Generate PMTiles
            pmtiles_urls = self.step_9_generate_pmtiles(geojson_files)
            
            # Step 10: Update metadata
            self.step_10_update_metadata(converted_files, pmtiles_urls)
            
            # Step 11: Finalize
            self.step_11_finalize()
            
            logger.info("PMN compilation completed successfully!")
            
            # Process historical years (2021 to current year)
            logger.info("Starting historical years processing...")
            self.process_historical_years()
            
        except Exception as e:
            logger.error(f"PMN compilation failed: {e}")
            self._update_progress(0, 'FAILED')
            raise

def main():
    """Main function"""
    if len(sys.argv) != 3:
        print("Usage: python compile_pmn.py <process_id> <year>")
        sys.exit(1)
    
    process_id = sys.argv[1]
    year = int(sys.argv[2])
    
    compiler = PMNCompiler(process_id, year)
    compiler.run()


if __name__ == "__main__":
    main()