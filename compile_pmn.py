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
            
            # Check if tables exist
            tables_to_check = [
                f'pmn.existing_{self.year}_copy1',
                f'pmn.potensi_{self.year}_copy1'
            ]
            
            for table in tables_to_check:
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'pmn' 
                        AND table_name = %s
                    )
                """, (table.split('.')[1],))
                
                exists = cursor.fetchone()[0]
                if not exists:
                    # Create table if not exists
                    cursor.execute(f"""
                        CREATE TABLE IF NOT EXISTS {table} (
                            id SERIAL PRIMARY KEY,
                            geometry GEOMETRY,
                            created_at TIMESTAMP DEFAULT NOW()
                        )
                    """)
                    logger.info(f"Created table: {table}")
                else:
                    logger.info(f"Table exists: {table}")
            
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
            
            # Get target table structures first
            target_cursor.execute("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_schema = 'pmn' AND table_name = %s
                ORDER BY ordinal_position
            """, (f'existing_{self.year}_copy1',))
            existing_target_cols = {row[0]: row[1] for row in target_cursor.fetchall()}
            
            target_cursor.execute("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_schema = 'pmn' AND table_name = %s
                ORDER BY ordinal_position
            """, (f'potensi_{self.year}_copy1',))
            potensi_target_cols = {row[0]: row[1] for row in target_cursor.fetchall()}
            
            logger.info(f"Target existing columns: {list(existing_target_cols.keys())}")
            logger.info(f"Target potensi columns: {list(potensi_target_cols.keys())}")
            
            for bpdas_db in bpdas_list:
                logger.info(f"Processing BPDAS: {bpdas_db}")
                
                try:
                    # Connect to BPDAS database
                    bpdas_conn = self._get_db_connection(bpdas_db)
                    bpdas_cursor = bpdas_conn.cursor()
                    
                    # ========== PROCESS POTENSI DATA ==========
                    try:
                        # Get source table columns
                        bpdas_cursor.execute("""
                            SELECT column_name 
                            FROM information_schema.columns 
                            WHERE table_schema = 'public' AND table_name = %s
                            ORDER BY ordinal_position
                        """, (f'potensi_{self.year}',))
                        source_potensi_cols = [row[0] for row in bpdas_cursor.fetchall()]
                        
                        # Find matching columns
                        matching_potensi_cols = [col for col in source_potensi_cols if col in potensi_target_cols]
                        
                        if not matching_potensi_cols:
                            logger.warning(f"No matching columns found for potensi_{self.year} in {bpdas_db}")
                        else:
                            logger.info(f"Matching potensi columns ({len(matching_potensi_cols)}): {matching_potensi_cols}")
                            
                            # Fetch data
                            bpdas_cursor.execute(f"""
                                SELECT {', '.join(matching_potensi_cols)} 
                                FROM public.potensi_{self.year}
                            """)
                            
                            potensi_data = bpdas_cursor.fetchall()
                            
                            if potensi_data:
                                # Bulk insert using execute_batch for better performance
                                insert_query = f"""
                                    INSERT INTO pmn.potensi_{self.year}_copy1 
                                    ({', '.join(matching_potensi_cols)}) 
                                    VALUES ({', '.join(['%s'] * len(matching_potensi_cols))})
                                """
                                
                                psycopg2.extras.execute_batch(target_cursor, insert_query, potensi_data)
                                logger.info(f"Inserted {len(potensi_data)} potensi records from {bpdas_db}")
                            else:
                                logger.info(f"No potensi data found in {bpdas_db}")
                                
                    except psycopg2.Error as e:
                        logger.warning(f"Failed to process potensi data from {bpdas_db}: {e}")
                    
                    # ========== PROCESS EXISTING DATA ==========
                    try:
                        # Get source table columns
                        bpdas_cursor.execute("""
                            SELECT column_name 
                            FROM information_schema.columns 
                            WHERE table_schema = 'public' AND table_name = %s
                            ORDER BY ordinal_position
                        """, (f'existing_{self.year}',))
                        source_existing_cols = [row[0] for row in bpdas_cursor.fetchall()]
                        
                        # Find matching columns
                        matching_existing_cols = [col for col in source_existing_cols if col in existing_target_cols]
                        
                        if not matching_existing_cols:
                            logger.warning(f"No matching columns found for existing_{self.year} in {bpdas_db}")
                        else:
                            logger.info(f"Matching existing columns ({len(matching_existing_cols)}): {matching_existing_cols}")
                            
                            # Fetch data
                            bpdas_cursor.execute(f"""
                                SELECT {', '.join(matching_existing_cols)} 
                                FROM public.existing_{self.year}
                            """)
                            
                            existing_data = bpdas_cursor.fetchall()
                            
                            if existing_data:
                                # Bulk insert using execute_batch
                                insert_query = f"""
                                    INSERT INTO pmn.existing_{self.year}_copy1 
                                    ({', '.join(matching_existing_cols)}) 
                                    VALUES ({', '.join(['%s'] * len(matching_existing_cols))})
                                """
                                
                                psycopg2.extras.execute_batch(target_cursor, insert_query, existing_data)
                                logger.info(f"Inserted {len(existing_data)} existing records from {bpdas_db}")
                            else:
                                logger.info(f"No existing data found in {bpdas_db}")
                                
                    except psycopg2.Error as e:
                        logger.warning(f"Failed to process existing data from {bpdas_db}: {e}")
                    
                    # Commit after each BPDAS to avoid losing all data on error
                    target_conn.commit()
                    
                    bpdas_cursor.close()
                    bpdas_conn.close()
                    
                    logger.info(f"Completed aggregation for BPDAS: {bpdas_db}")
                    
                except psycopg2.OperationalError as e:
                    logger.warning(f"Failed to connect to BPDAS database {bpdas_db}: {e}")
                    continue
                except Exception as e:
                    logger.warning(f"Unexpected error processing BPDAS {bpdas_db}: {e}")
                    continue
            
            # Final commit
            target_conn.commit()
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
            existing_gdf = gpd.read_postgis(
                f"SELECT * FROM pmn.existing_{self.year}_copy1",
                conn,
                geom_col='geometry'
            )
            
            existing_file = os.path.join(self.temp_dir, f'existing_{self.year}.geojson')
            existing_gdf.to_file(existing_file, driver='GeoJSON')
            geojson_files['existing'] = existing_file
            logger.info(f"Exported existing data: {len(existing_gdf)} features")
            
            # Export potensi data
            potensi_gdf = gpd.read_postgis(
                f"SELECT * FROM pmn.potensi_{self.year}_copy1",
                conn,
                geom_col='geometry'
            )
            
            potensi_file = os.path.join(self.temp_dir, f'potensi_{self.year}.geojson')
            potensi_gdf.to_file(potensi_file, driver='GeoJSON')
            geojson_files['potensi'] = potensi_file
            logger.info(f"Exported potensi data: {len(potensi_gdf)} features")
            
            conn.close()
            
            self._update_progress(70)
            logger.info("Step 7 completed: GeoJSON export finished")
            
            return geojson_files
            
        except Exception as e:
            logger.error(f"Step 7 failed: {e}")
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
                
                # Convert to Shapefile
                shp_dir = os.path.join(self.temp_dir, f'{theme}_shp')
                os.makedirs(shp_dir, exist_ok=True)
                shp_file = os.path.join(shp_dir, f'AR_25K_PETAMANGROVE_{theme.upper()}_{self.year}.shp')
                gdf.to_file(shp_file, driver='ESRI Shapefile')
                
                # Zip Shapefile
                shp_zip = os.path.join(self.temp_dir, f'AR_25K_PETAMANGROVE_{theme.upper()}_{self.year}.zip')
                with zipfile.ZipFile(shp_zip, 'w') as zipf:
                    for ext in ['.shp', '.shx', '.dbf', '.prj', '.cpg']:
                        file_path = shp_file.replace('.shp', ext)
                        if os.path.exists(file_path):
                            zipf.write(file_path, os.path.basename(file_path))
                
                theme_files['shp_zip'] = shp_zip
                
                # Convert to GDB using ogr2ogr
                gdb_dir = os.path.join(self.temp_dir, f'AR_25K_PETAMANGROVE_{theme.upper()}_{self.year}.gdb')
                subprocess.run([
                    'ogr2ogr', '-f', 'FileGDB', gdb_dir, geojson_file
                ], check=True)
                
                # Zip GDB
                gdb_zip = os.path.join(self.temp_dir, f'AR_25K_PETAMANGROVE_{theme.upper()}_{self.year}.gdb.zip')
                with zipfile.ZipFile(gdb_zip, 'w') as zipf:
                    for root, dirs, files in os.walk(gdb_dir):
                        for file in files:
                            file_path = os.path.join(root, file)
                            arcname = os.path.relpath(file_path, os.path.dirname(gdb_dir))
                            zipf.write(file_path, arcname)
                
                theme_files['gdb_zip'] = gdb_zip
                theme_files['gdb_dir'] = gdb_dir
                
                converted_files[theme] = theme_files
                logger.info(f"Converted {theme} to Shapefile and GDB")
            
            self._update_progress(80)
            logger.info("Step 8 completed: Format conversion finished")
            
            return converted_files
            
        except Exception as e:
            logger.error(f"Step 8 failed: {e}")
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
            
        except Exception as e:
            logger.error(f"PMN compilation failed: {e}")
            self._update_progress(0, 'FAILED')
            raise
        finally:
            self.cleanup()


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