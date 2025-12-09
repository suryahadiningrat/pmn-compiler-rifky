#!/usr/bin/env python3
"""
PMN Compiler Script
Mengkompilasi data mangrove dari berbagai database BPDAS ke dalam format final
(GeoJSON, Shapefile, GDB, PMTiles) dengan progress tracking.
"""

import os
import sys
import re
import json
import hashlib
import logging
import zipfile
import tempfile
import subprocess
import unicodedata
from datetime import datetime
from typing import List, Dict, Any, Optional
from shapely.ops import unary_union, polygonize
from shapely.geometry import MultiPolygon
import requests
import psycopg2
import psycopg2.extras
import geopandas as gpd
import boto3
from botocore.exceptions import ClientError
import pandas as pd
from pathlib import Path

# Konfigurasi Database
DB_CONFIG = {
    'host': '52.74.112.75',
    'port': 5432,
    'user': 'pg',
    'password': '~nagha2025yasha@~'
}

# Konfigurasi AWS S3
S3_CONFIG = {
    'access_key': '',
    'secret_key': '',
    'endpoint_url': 'https://s3.ap-southeast-3.amazonaws.com',
    'public_host': 'https://idpm-bucket.s3.ap-southeast-3.amazonaws.com',
    'bucket': 'idpm-bucket',
    'region': 'ap-southeast-3',
    's3_prefix': 'idpm'  # Prefix for all S3 paths: idpm/layers/, idpm/pmn-result/
}

# Konfigurasi PMTiles per BPDAS
PMTILES_BPDAS_CONFIG = {
    'minzoom': 4,
    'maxzoom': 17,
    's3_prefix': 'idpm/static/layer'  # Base path di S3: idpm/static/layer/{theme}/{year}/bpdas/
}

# Mapping nama database BPDAS ke slug yang benar
# Key: nama database (lowercase), Value: slug untuk filename
BPDAS_SLUG_MAP = {
    'agamkuantan': 'agam_kuantan',
    'akemalamo': 'ake_malamo',
    'asahanbarumun': 'asahan_barumun',
    'barito': 'barito',
    'batanghari': 'batanghari',
    'baturusacerucuk': 'baturusa_cerucuk',
    'benainnoelmina': 'benain_noelmina',
    'bonebolango': 'bone_bolango',
    'bonelimboto': 'bone_limboto',
    'brantassampean': 'brantas_sampean',
    'cimanukcitanduy': 'cimanuk_citanduy',
    'citarumciliwung': 'citarum_ciliwung',
    'dodokanmoyosari': 'dodokan_moyosari',
    'indragirirokan': 'indragiri_rokan',
    'jeneberangsaddang': 'jeneberang_saddang',
    'kahayan': 'kahayan',
    'kapuas': 'kapuas',
    'karama': 'karama',
    'ketahun': 'ketahun',
    'konaweha': 'konaweha',
    'kruengaceh': 'krueng_aceh',
    'lariangmamasa': 'lariang_mamasa',
    'mahakamberau': 'mahakam_berau',
    'memberamo': 'memberamo',
    'musi': 'musi',
    'paluposo': 'palu_poso',
    'pemalijratun': 'pemali_jratun',
    'remuransiki': 'remu_ransiki',
    'sampara': 'sampara',
    'seijangduriangkang': 'sei_jang_duriangkang',
    'serayuopakprogo': 'serayu_opak_progo',
    'solo': 'solo',
    'tondano': 'tondano',
    'undaanyar': 'unda_anyar',
    'waehapubatumerah': 'waehapu_batu_merah',
    'wampuseiular': 'wampu_sei_ular',
    'wayseputihwaysekampung': 'way_seputih_way_sekampung',
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
        self.s3_client = None
        self.temp_dir = None
        self.accessible_bpdas = []  # Add this line
        
        # Initialize S3 client
        self._init_s3()
        
        # Create temporary directory
        self.temp_dir = tempfile.mkdtemp(prefix=f'pmn_compiler_{year}_')
        logger.info(f"Temporary directory created: {self.temp_dir}")

    def _init_s3(self):
        """Initialize AWS S3 client"""
        try:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=S3_CONFIG['access_key'],
                aws_secret_access_key=S3_CONFIG['secret_key'],
                region_name=S3_CONFIG['region']
            )
            logger.info("AWS S3 client initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize S3 client: {e}")
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

    def _check_s3_connection(self) -> bool:
        """Check AWS S3 connection and bucket accessibility"""
        logger.info("Checking AWS S3 connection...")
        
        try:
            # Check if bucket exists by listing objects
            self.s3_client.head_bucket(Bucket=S3_CONFIG['bucket'])
            logger.info(f"S3 bucket '{S3_CONFIG['bucket']}' is accessible")
            
            # Try to list objects (minimal operation to verify permissions)
            self.s3_client.list_objects_v2(Bucket=S3_CONFIG['bucket'], Prefix='test/', MaxKeys=1)
            logger.info("S3 read/write permissions verified")
            
            return True
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                logger.error(f"S3 bucket '{S3_CONFIG['bucket']}' does not exist")
            else:
                logger.error(f"S3 error: {e}")
            return False
        except Exception as e:
            logger.error(f"S3 connection check failed: {e}")
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
            # 1. Check S3 connection
            if not self._check_s3_connection():
                logger.error("PREFLIGHT FAILED: S3 connection check failed")
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
    
    def step_5_clean_s3_files(self):
        """Step 5: Bersihkan File Lama di S3 (50%)"""
        logger.info("Step 5: Cleaning old S3 files...")
        
        try:
            # List and delete files in idpm/pmn-result/{year}/ path
            prefix = f"{S3_CONFIG['s3_prefix']}/pmn-result/{self.year}/"
            
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=S3_CONFIG['bucket'], Prefix=prefix)
            
            deleted_count = 0
            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        self.s3_client.delete_object(Bucket=S3_CONFIG['bucket'], Key=obj['Key'])
                        deleted_count += 1
                        logger.info(f"Deleted: {obj['Key']}")
            
            logger.info(f"Deleted {deleted_count} files from S3")
            
            self._update_progress(50)
            logger.info("Step 5 completed: S3 files cleaned")
            
        except Exception as e:
            logger.error(f"Step 5 failed: {e}")
            raise

    def step_6_delete_old_pmtiles(self):
        """Step 6: Hapus PMTiles Lama (60%)"""
        logger.info("Step 6: Deleting old PMTiles...")
        
        try:
            pmtiles_files = [
                f"{S3_CONFIG['s3_prefix']}/layers/EXISTING{self.year}.pmtiles",
                f"{S3_CONFIG['s3_prefix']}/layers/POTENSI{self.year}.pmtiles"
            ]
            
            for pmtiles_file in pmtiles_files:
                try:
                    self.s3_client.delete_object(Bucket=S3_CONFIG['bucket'], Key=pmtiles_file)
                    logger.info(f"Deleted PMTiles: {pmtiles_file}")
                except ClientError as e:
                    if e.response['Error']['Code'] == '404':
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
                
                # Upload to S3 - dengan ekstensi .pmtiles
                s3_path_with_ext = f"{S3_CONFIG['s3_prefix']}/layers/{theme.upper()}{self.year}.pmtiles"
                self.s3_client.upload_file(
                    pmtiles_file,
                    S3_CONFIG['bucket'],
                    s3_path_with_ext
                )
                logger.info(f"  ✅ Uploaded: {s3_path_with_ext}")
                
                # Upload to S3 - tanpa ekstensi (untuk legacy/geoportal compatibility)
                s3_path_no_ext = f"{S3_CONFIG['s3_prefix']}/layers/{theme.upper()}{self.year}"
                self.s3_client.upload_file(
                    pmtiles_file,
                    S3_CONFIG['bucket'],
                    s3_path_no_ext
                )
                logger.info(f"  ✅ Uploaded: {s3_path_no_ext}"))
                
                # URL tanpa ekstensi untuk geoportal.layers
                pmtiles_url = f"{S3_CONFIG['public_host']}/{s3_path_no_ext}"
                pmtiles_urls[theme] = pmtiles_url
                
                logger.info(f"Generated and uploaded PMTiles for {theme}: {pmtiles_url}")
            
            self._update_progress(90)
            logger.info("Step 9 completed: PMTiles generation finished")
            
            return pmtiles_urls
            
        except Exception as e:
            logger.error(f"Step 9 failed: {e}")
            raise

    def _slugify(self, value: str) -> str:
        """
        Normalisasi & 'slugify' nama -> lowercase, underscore, alfanumerik.
        Digunakan untuk membuat nama file yang aman.
        """
        value = str(value)
        value = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
        value = value.lower().strip()
        value = re.sub(r"\s+", "_", value)               # spasi -> underscore
        value = re.sub(r"[^a-z0-9_]+", "", value)        # hanya a-z0-9_
        value = re.sub(r"_+", "_", value).strip("_")     # rapikan underscore
        return value or "unknown"

    def _escape_sql_literal(self, val: str) -> str:
        """
        Escape single quote untuk literal SQL (ogr2ogr tidak support bind params).
        """
        return str(val).replace("'", "''")

    def _build_ogr2ogr_pmtiles_cmd(self, output_file: str, layer_name: str, 
                                    schema: str, table: str, bpdas_column: str, 
                                    bpdas_value: str) -> list:
        """
        Build ogr2ogr command untuk generate PMTiles dengan filter BPDAS.
        
        Args:
            output_file: Path ke file output PMTiles
            layer_name: Nama layer dalam PMTiles
            schema: Schema database (pmn)
            table: Nama tabel (existing_{year}_copy1 atau potensi_{year}_copy1)
            bpdas_column: Nama kolom BPDAS
            bpdas_value: Nilai BPDAS untuk filter
        """
        # SQL query dengan filter BPDAS
        val_sql = self._escape_sql_literal(bpdas_value)
        sql = f'SELECT * FROM {schema}.{table} WHERE "{bpdas_column}" = \'{val_sql}\''
        
        # Connection string ke PostgreSQL
        pg_conn = f"PG:host={DB_CONFIG['host']} port={DB_CONFIG['port']} dbname=postgres user={DB_CONFIG['user']} password={DB_CONFIG['password']}"
        
        # Susun command ogr2ogr
        cmd = [
            "ogr2ogr", "-progress",
            "-dsco", f"MINZOOM={PMTILES_BPDAS_CONFIG['minzoom']}",
            "-dsco", f"MAXZOOM={PMTILES_BPDAS_CONFIG['maxzoom']}",
            "-f", "PMTiles",
            "-overwrite",
            output_file,
            pg_conn,
            "-sql", sql,
            "-nln", layer_name,
            "-nlt", "PROMOTE_TO_MULTI"
        ]
        return cmd

    def step_9b_generate_pmtiles_per_bpdas(self) -> Dict[str, List[str]]:
        """
        Step 9B: Generate PMTiles per BPDAS (92%)
        
        Menghasilkan file PMTiles terpisah untuk setiap BPDAS yang ada di data.
        Output di-upload ke S3 path: static/layer/{theme}/{year}/bpdas/{bpdas_slug}_{year}.pmtiles
        
        Returns:
            Dict dengan key 'existing' dan 'potensi', masing-masing berisi list URL PMTiles
        """
        logger.info("Step 9B: Generating PMTiles per BPDAS...")
        logger.info("=" * 60)
        
        try:
            conn = self._get_db_connection()
            cursor = conn.cursor()
            
            pmtiles_urls = {'existing': [], 'potensi': []}
            pmtiles_local_dir = os.path.join(self.temp_dir, 'pmtiles_bpdas')
            os.makedirs(pmtiles_local_dir, exist_ok=True)
            
            total_generated = 0
            total_failed = 0
            
            for theme in ['existing', 'potensi']:
                table_name = f'{theme}_{self.year}_copy1'
                
                # Get distinct BPDAS values dari tabel
                cursor.execute(f"""
                    SELECT DISTINCT bpdas 
                    FROM pmn.{table_name} 
                    WHERE bpdas IS NOT NULL AND bpdas != ''
                    ORDER BY bpdas
                """)
                
                bpdas_list = [row[0] for row in cursor.fetchall()]
                
                if not bpdas_list:
                    logger.warning(f"No BPDAS found in pmn.{table_name}")
                    continue
                
                logger.info(f"\n📊 {theme.upper()}: Found {len(bpdas_list)} unique BPDAS")
                
                for idx, bpdas_name in enumerate(bpdas_list, start=1):
                    # Gunakan BPDAS_SLUG_MAP jika tersedia, fallback ke slugify
                    bpdas_key = bpdas_name.lower().replace(' ', '').replace('_', '').replace('-', '')
                    bpdas_slug = BPDAS_SLUG_MAP.get(bpdas_key, self._slugify(bpdas_name))
                    
                    layer_name = f"{theme}_{bpdas_slug}_{self.year}"
                    local_file = os.path.join(pmtiles_local_dir, f"{bpdas_slug}_{self.year}.pmtiles")
                    # Path: static/layer/{theme}/{year}/bpdas/{bpdas_slug}_{year}.pmtiles
                    s3_path = f"{PMTILES_BPDAS_CONFIG['s3_prefix']}/{theme}/{self.year}/bpdas/{bpdas_slug}_{self.year}.pmtiles"
                    
                    logger.info(f"  [{idx}/{len(bpdas_list)}] Processing: {bpdas_name} -> {bpdas_slug}")
                    
                    try:
                        # Build dan jalankan ogr2ogr command
                        cmd = self._build_ogr2ogr_pmtiles_cmd(
                            output_file=local_file,
                            layer_name=layer_name,
                            schema='pmn',
                            table=table_name,
                            bpdas_column='bpdas',
                            bpdas_value=bpdas_name
                        )
                        
                        result = subprocess.run(cmd, capture_output=True, text=True)
                        
                        if result.returncode != 0:
                            logger.error(f"    ❌ ogr2ogr failed: {result.stderr}")
                            total_failed += 1
                            continue
                        
                        # Check if file was created and has content
                        if not os.path.exists(local_file) or os.path.getsize(local_file) == 0:
                            logger.warning(f"    ⚠️  PMTiles file empty or not created")
                            total_failed += 1
                            continue
                        
                        # Upload ke S3 - dengan ekstensi .pmtiles
                        self.s3_client.upload_file(
                            local_file,
                            S3_CONFIG['bucket'],
                            s3_path
                        )
                        
                        # Upload ke S3 - tanpa ekstensi (untuk geoportal compatibility)
                        s3_path_no_ext = s3_path.replace('.pmtiles', '')
                        self.s3_client.upload_file(
                            local_file,
                            S3_CONFIG['bucket'],
                            s3_path_no_ext
                        )
                        
                        pmtiles_url = f"{S3_CONFIG['public_host']}/{s3_path}"
                        pmtiles_urls[theme].append(pmtiles_url)
                        
                        file_size_mb = os.path.getsize(local_file) / (1024 * 1024)
                        logger.info(f"    ✅ Uploaded: {s3_path} ({file_size_mb:.2f} MB)")
                        logger.info(f"    ✅ Uploaded: {s3_path_no_ext} (no ext)")
                        total_generated += 1
                        
                        # Cleanup local file to save space
                        os.remove(local_file)
                        
                    except FileNotFoundError:
                        logger.error("    ❌ ogr2ogr not found! Please install GDAL with PMTiles driver.")
                        total_failed += 1
                    except subprocess.CalledProcessError as e:
                        logger.error(f"    ❌ Failed: {e}")
                        total_failed += 1
                    except ClientError as e:
                        logger.error(f"    ❌ S3 upload failed: {e}")
                        total_failed += 1
                    except Exception as e:
                        logger.error(f"    ❌ Unexpected error: {e}")
                        total_failed += 1
            
            cursor.close()
            conn.close()
            
            # Summary
            logger.info("\n" + "=" * 60)
            logger.info("PMTILES PER BPDAS SUMMARY:")
            logger.info(f"  Total generated: {total_generated}")
            logger.info(f"  Total failed: {total_failed}")
            logger.info(f"  Existing PMTiles: {len(pmtiles_urls['existing'])}")
            logger.info(f"  Potensi PMTiles: {len(pmtiles_urls['potensi'])}")
            logger.info("=" * 60)
            
            self._update_progress(92)
            logger.info("Step 9B completed: PMTiles per BPDAS generation finished")
            
            return pmtiles_urls
            
        except Exception as e:
            logger.error(f"Step 9B failed: {e}")
            import traceback
            logger.error(traceback.format_exc())
            raise

    def step_9c_register_geoportal_layers(self, pmtiles_urls: Dict[str, str], pmtiles_bpdas_urls: Dict[str, List[str]]):
        """
        Step 9C: Register layers to geoportal.layers table (94%)
        
        Insert layer metadata ke table postgres.geoportal.layers untuk:
        1. PMTiles Nasional (Existing & Potensi)
        2. PMTiles per BPDAS
        
        Args:
            pmtiles_urls: Dict dengan URL PMTiles nasional {'existing': url, 'potensi': url}
            pmtiles_bpdas_urls: Dict dengan list URL PMTiles per BPDAS
        """
        logger.info("Step 9C: Registering layers to geoportal.layers...")
        logger.info("=" * 60)
        
        try:
            conn = self._get_db_connection()
            cursor = conn.cursor()
            
            layers_inserted = 0
            layers_updated = 0
            
            # 1. Register PMTiles Nasional
            for theme in ['existing', 'potensi']:
                if theme not in pmtiles_urls:
                    continue
                    
                url = pmtiles_urls[theme]
                theme_display = 'Existing' if theme == 'existing' else 'Potensi'
                layer_name = f"{theme_display} Mangrove {self.year}"
                
                # Prepare layer JSON
                layer_json = json.dumps({
                    "url": url,
                    "format": "VectorTiles",
                    "maxzoom": "15",
                    "minzoom": "5",
                    "name": layer_name
                })
                
                # Check if layer already exists
                cursor.execute("""
                    SELECT id FROM geoportal.layers 
                    WHERE nama = %s AND type = 'layers'
                """, (layer_name,))
                
                existing = cursor.fetchone()
                
                if existing:
                    # Update existing layer
                    cursor.execute("""
                        UPDATE geoportal.layers 
                        SET layer = %s, date_created = %s, description = %s
                        WHERE id = %s
                    """, (layer_json, datetime.now().date(), layer_name, existing[0]))
                    layers_updated += 1
                    logger.info(f"  ✅ Updated: {layer_name} (id={existing[0]})")
                else:
                    # Insert new layer
                    cursor.execute("""
                        INSERT INTO geoportal.layers 
                        (layer, style, public, date_created, description, owner, nama, avatar, fields, status, immortal, type)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING id
                    """, (
                        layer_json,           # layer (json)
                        '{}',                 # style (json)
                        False,                # public (bool)
                        datetime.now().date(), # date_created (date)
                        layer_name,           # description (text)
                        '',                   # owner (varchar)
                        layer_name,           # nama (text)
                        '/icons/geo-2.png',   # avatar (text)
                        '{}',                 # fields (json)
                        'READY',              # status (text) - values: READY, BELUMPROSES, etc
                        False,                # immortal (bool)
                        'layers'              # type (varchar)
                    ))
                    new_id = cursor.fetchone()[0]
                    layers_inserted += 1
                    logger.info(f"  ✅ Inserted: {layer_name} (id={new_id})")
            
            conn.commit()
            cursor.close()
            conn.close()
            
            # Summary
            logger.info("\n" + "=" * 60)
            logger.info("GEOPORTAL LAYERS REGISTRATION SUMMARY:")
            logger.info(f"  Layers inserted: {layers_inserted}")
            logger.info(f"  Layers updated: {layers_updated}")
            logger.info(f"  Total: {layers_inserted + layers_updated}")
            logger.info("=" * 60)
            
            self._update_progress(94)
            logger.info("Step 9C completed: Geoportal layers registration finished")
            
        except Exception as e:
            logger.error(f"Step 9C failed: {e}")
            import traceback
            logger.error(traceback.format_exc())
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
                
                # Upload files to S3
                gdb_s3_path = f"{S3_CONFIG['s3_prefix']}/pmn-result/{self.year}/AR_25K_PETAMANGROVE_{theme.upper()}_{self.year}.gdb.zip"
                shp_s3_path = f"{S3_CONFIG['s3_prefix']}/pmn-result/{self.year}/AR_25K_PETAMANGROVE_{theme.upper()}_{self.year}.zip"
                
                self.s3_client.upload_file(
                    converted_files[theme]['gdb_zip'],
                    S3_CONFIG['bucket'],
                    gdb_s3_path
                )
                
                self.s3_client.upload_file(
                    converted_files[theme]['shp_zip'],
                    S3_CONFIG['bucket'],
                    shp_s3_path
                )
                
                # Construct URLs
                gdb_url = f"{S3_CONFIG['public_host']}/{gdb_s3_path}"
                shp_url = f"{S3_CONFIG['public_host']}/{shp_s3_path}"
                
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

    def _check_file_exists_in_s3(self, object_path: str) -> bool:
        """Check if file exists in S3"""
        try:
            self.s3_client.head_object(Bucket=S3_CONFIG['bucket'], Key=object_path)
            return True
        except ClientError:
            return False

    def _check_year_files_exist(self, year: int) -> Dict[str, bool]:
        """Check which files exist for a specific year"""
        files_status = {}
        
        # Check PMTiles
        pmtiles_existing = f"{S3_CONFIG['s3_prefix']}/layers/EXISTING{year}.pmtiles"
        pmtiles_potensi = f"{S3_CONFIG['s3_prefix']}/layers/POTENSI{year}.pmtiles"
        files_status['pmtiles_existing'] = self._check_file_exists_in_s3(pmtiles_existing)
        files_status['pmtiles_potensi'] = self._check_file_exists_in_s3(pmtiles_potensi)
        
        # Check Shapefile
        shp_existing = f"{S3_CONFIG['s3_prefix']}/pmn-result/{year}/AR_25K_PETAMANGROVE_EXISTING_{year}.zip"
        shp_potensi = f"{S3_CONFIG['s3_prefix']}/pmn-result/{year}/AR_25K_PETAMANGROVE_POTENSI_{year}.zip"
        files_status['shp_existing'] = self._check_file_exists_in_s3(shp_existing)
        files_status['shp_potensi'] = self._check_file_exists_in_s3(shp_potensi)
        
        # Check GDB
        gdb_existing = f"{S3_CONFIG['s3_prefix']}/pmn-result/{year}/AR_25K_PETAMANGROVE_EXISTING_{year}.gdb.zip"
        gdb_potensi = f"{S3_CONFIG['s3_prefix']}/pmn-result/{year}/AR_25K_PETAMANGROVE_POTENSI_{year}.gdb.zip"
        files_status['gdb_existing'] = self._check_file_exists_in_s3(gdb_existing)
        files_status['gdb_potensi'] = self._check_file_exists_in_s3(gdb_potensi)
        
        return files_status

    def _validate_geojson_file(self, geojson_file: str) -> bool:
        """Validate if a GeoJSON file is valid and contains features"""
        try:
            import json
            with open(geojson_file, 'r') as f:
                data = json.load(f)
            
            if data.get('type') != 'FeatureCollection':
                logger.warning(f"GeoJSON file is not a FeatureCollection")
                return False
            
            features = data.get('features', [])
            if not features or len(features) == 0:
                logger.warning(f"GeoJSON file contains no features")
                return False
            
            logger.info(f"GeoJSON validation passed: {len(features)} features found")
            return True
            
        except json.JSONDecodeError as e:
            logger.error(f"GeoJSON file contains invalid JSON: {e}")
            return False
        except Exception as e:
            logger.error(f"Error validating GeoJSON file: {e}")
            return False

    def _validate_shapefile(self, shp_zip: str) -> bool:
        """Validate if a Shapefile ZIP contains valid data"""
        try:
            import zipfile
            import tempfile
            
            # Check if ZIP file exists and has reasonable size
            if not os.path.exists(shp_zip) or os.path.getsize(shp_zip) < 1000:  # At least 1KB
                logger.warning(f"Shapefile ZIP is missing or too small: {shp_zip}")
                return False
            
            # Check ZIP contents
            with zipfile.ZipFile(shp_zip, 'r') as zip_ref:
                files = zip_ref.namelist()
                required_extensions = ['.shp', '.shx', '.dbf']
                
                has_required = all(any(f.endswith(ext) for f in files) for ext in required_extensions)
                if not has_required:
                    logger.warning(f"Shapefile ZIP missing required files (.shp, .shx, .dbf)")
                    return False
            
            # Try to read with geopandas
            import geopandas as gpd
            temp_dir = tempfile.mkdtemp()
            try:
                with zipfile.ZipFile(shp_zip, 'r') as zip_ref:
                    zip_ref.extractall(temp_dir)
                
                shp_file = None
                for file in os.listdir(temp_dir):
                    if file.endswith('.shp'):
                        shp_file = os.path.join(temp_dir, file)
                        break
                
                if shp_file:
                    gdf = gpd.read_file(shp_file)
                    if len(gdf) == 0:
                        logger.warning(f"Shapefile contains no features")
                        return False
                    logger.info(f"Shapefile validation passed: {len(gdf)} features")
                    return True
                else:
                    logger.warning(f"No .shp file found in ZIP")
                    return False
                    
            finally:
                import shutil
                shutil.rmtree(temp_dir, ignore_errors=True)
                
        except Exception as e:
            logger.error(f"Error validating Shapefile: {e}")
            return False

    def _validate_gdb(self, gdb_zip: str) -> bool:
        """Validate if a GDB ZIP contains valid data"""
        try:
            import zipfile
            import tempfile
            
            # Check if ZIP file exists and has reasonable size
            if not os.path.exists(gdb_zip) or os.path.getsize(gdb_zip) < 1000:  # At least 1KB
                logger.warning(f"GDB ZIP is missing or too small: {gdb_zip}")
                return False
            
            # Check ZIP contents for GDB structure
            with zipfile.ZipFile(gdb_zip, 'r') as zip_ref:
                files = zip_ref.namelist()
                
                # Look for GDB files
                has_gdb_files = any(f.endswith('.gdb/') or '.gdb/' in f for f in files)
                if not has_gdb_files:
                    logger.warning(f"GDB ZIP does not contain .gdb structure")
                    return False
            
            # Try to read with geopandas
            import geopandas as gpd
            temp_dir = tempfile.mkdtemp()
            try:
                with zipfile.ZipFile(gdb_zip, 'r') as zip_ref:
                    zip_ref.extractall(temp_dir)
                
                # Find GDB directory
                gdb_dir = None
                for item in os.listdir(temp_dir):
                    item_path = os.path.join(temp_dir, item)
                    if os.path.isdir(item_path) and item.endswith('.gdb'):
                        gdb_dir = item_path
                        break
                
                if gdb_dir:
                    # List layers in GDB
                    import fiona
                    layers = fiona.listlayers(gdb_dir)
                    if not layers:
                        logger.warning(f"GDB contains no layers")
                        return False
                    
                    # Read first layer to validate
                    gdf = gpd.read_file(gdb_dir, layer=layers[0])
                    if len(gdf) == 0:
                        logger.warning(f"GDB layer contains no features")
                        return False
                    
                    logger.info(f"GDB validation passed: {len(layers)} layers, {len(gdf)} features in first layer")
                    return True
                else:
                    logger.warning(f"No .gdb directory found in ZIP")
                    return False
                    
            finally:
                import shutil
                shutil.rmtree(temp_dir, ignore_errors=True)
                
        except Exception as e:
            logger.error(f"Error validating GDB: {e}")
            return False

    def _convert_pmtiles_to_formats(self, year: int):
        """Convert PMTiles to Shapefile and GDB for specific year"""
        logger.info(f"Converting PMTiles to Shapefile and GDB for year {year}")
        
        # Check which PMTiles exist (at least one must exist)
        pmtiles_existing = f"{S3_CONFIG['s3_prefix']}/layers/EXISTING{year}.pmtiles"
        pmtiles_potensi = f"{S3_CONFIG['s3_prefix']}/layers/POTENSI{year}.pmtiles"
        
        existing_exists = self._check_file_exists_in_s3(pmtiles_existing)
        potensi_exists = self._check_file_exists_in_s3(pmtiles_potensi)
        
        if not (existing_exists or potensi_exists):
            logger.warning(f"No PMTiles files found for year {year}, skipping conversion")
            return
        
        logger.info(f"PMTiles availability for {year}: existing={existing_exists}, potensi={potensi_exists}")
        
        # Download PMTiles from S3
        temp_pmtiles_dir = os.path.join(self.temp_dir, f'pmtiles_{year}')
        os.makedirs(temp_pmtiles_dir, exist_ok=True)
        
        try:
            # Prepare download list
            pmtiles_to_process = []
            
            # Download existing PMTiles if available
            if existing_exists:
                existing_pmtiles = os.path.join(temp_pmtiles_dir, f'existing_{year}.pmtiles')
                logger.info(f"Downloading existing PMTiles from S3: {pmtiles_existing}")
                self.s3_client.download_file(S3_CONFIG['bucket'], pmtiles_existing, existing_pmtiles)
                
                # Verify download
                if not os.path.exists(existing_pmtiles) or os.path.getsize(existing_pmtiles) == 0:
                    logger.error(f"Failed to download or empty existing PMTiles: {existing_pmtiles}")
                else:
                    existing_size = os.path.getsize(existing_pmtiles)
                    logger.info(f"✓ Downloaded existing PMTiles: {existing_pmtiles} ({existing_size:,} bytes)")
                    pmtiles_to_process.append(('existing', existing_pmtiles))
            
            # Download potensi PMTiles if available
            if potensi_exists:
                potensi_pmtiles = os.path.join(temp_pmtiles_dir, f'potensi_{year}.pmtiles')
                logger.info(f"Downloading potensi PMTiles from S3: {pmtiles_potensi}")
                self.s3_client.download_file(S3_CONFIG['bucket'], pmtiles_potensi, potensi_pmtiles)
                
                # Verify download
                if not os.path.exists(potensi_pmtiles) or os.path.getsize(potensi_pmtiles) == 0:
                    logger.error(f"Failed to download or empty potensi PMTiles: {potensi_pmtiles}")
                else:
                    potensi_size = os.path.getsize(potensi_pmtiles)
                    logger.info(f"✓ Downloaded potensi PMTiles: {potensi_pmtiles} ({potensi_size:,} bytes)")
                    pmtiles_to_process.append(('potensi', potensi_pmtiles))
            
            if not pmtiles_to_process:
                raise Exception(f"No valid PMTiles files downloaded for year {year}")
            
            logger.info(f"✅ PMTiles files ready for processing: {len(pmtiles_to_process)} files for year {year}")            # Convert PMTiles to GeoJSON first
            for theme, pmtiles_file in pmtiles_to_process:
                logger.info(f"Converting {theme} PMTiles to GeoJSON for year {year}")
                
                # Verify PMTiles file before conversion
                if not os.path.exists(pmtiles_file):
                    raise Exception(f"PMTiles file not found: {pmtiles_file}")
                
                pmtiles_size = os.path.getsize(pmtiles_file)
                if pmtiles_size == 0:
                    raise Exception(f"PMTiles file is empty: {pmtiles_file}")
                
                logger.info(f"Processing PMTiles file: {pmtiles_file} ({pmtiles_size:,} bytes)")
                
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
                    
                    if os.path.exists(geojson_file) and os.path.getsize(geojson_file) > 1000:  # At least 1KB
                        # Validate GeoJSON content
                        if self._validate_geojson_file(geojson_file):
                            logger.info(f"✅ pmtiles extract successful")
                            conversion_success = True
                        else:
                            logger.warning(f"pmtiles extract produced invalid GeoJSON")
                            if os.path.exists(geojson_file):
                                os.remove(geojson_file)
                except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired) as e:
                    logger.warning(f"pmtiles extract failed: {e}")
                
                # Method 2: Try tippecanoe-decode with proper tile extraction
                if not conversion_success:
                    try:
                        logger.info(f"Trying tippecanoe-decode tile extraction for {theme}...")
                        
                        # Use tippecanoe-decode to extract specific zoom level tiles and convert to GeoJSON
                        # First, get metadata to understand the PMTiles structure
                        metadata_result = subprocess.run([
                            'tippecanoe-decode', '-c', pmtiles_file
                        ], capture_output=True, text=True, timeout=30)
                        
                        if metadata_result.returncode == 0:
                            logger.info(f"PMTiles metadata retrieved successfully")
                            
                            # Try to extract tiles at zoom level 0 (lowest zoom with all data)
                            result = subprocess.run([
                                'tippecanoe-decode', '-z', '0', pmtiles_file
                            ], capture_output=True, text=True, timeout=300)
                            
                            # Check if we got valid GeoJSON output
                            if result.stdout and len(result.stdout) > 100:
                                # Validate the output is proper GeoJSON
                                try:
                                    import json
                                    json_data = json.loads(result.stdout)
                                    if json_data.get('type') == 'FeatureCollection':
                                        with open(geojson_file, 'w') as f:
                                            f.write(result.stdout)
                                        
                                        if os.path.exists(geojson_file) and os.path.getsize(geojson_file) > 0:
                                            logger.info(f"✅ tippecanoe-decode tile extraction successful (exit code: {result.returncode})")
                                            if result.returncode != 0:
                                                logger.warning(f"tippecanoe-decode warnings: {result.stderr}")
                                            conversion_success = True
                                        else:
                                            logger.error(f"tippecanoe-decode produced empty file")
                                    else:
                                        logger.error(f"tippecanoe-decode output is not valid GeoJSON FeatureCollection")
                                except json.JSONDecodeError as e:
                                    logger.error(f"tippecanoe-decode output is not valid JSON: {e}")
                            else:
                                logger.error(f"tippecanoe-decode produced no usable output")
                        else:
                            logger.warning(f"Failed to get PMTiles metadata: {metadata_result.stderr}")
                            
                    except subprocess.TimeoutExpired:
                        logger.error(f"tippecanoe-decode timeout")
                    except Exception as e:
                        logger.error(f"tippecanoe-decode error: {e}")
                
                # Method 2b: Try tippecanoe-decode with layer-specific extraction
                if not conversion_success:
                    try:
                        logger.info(f"Trying tippecanoe-decode layer extraction for {theme}...")
                        
                        # Try extracting with correct layer names based on PMTiles structure analysis
                        if theme.lower() == "existing":
                            layer_names = [
                                "MANGROVE_EKSISTING_IGT",   # Correct layer name from PMTiles analysis
                                f"{theme.upper()}_{year}",  # EXISTING_2021 (backup)
                                f"{theme.lower()}_{year}",  # existing_2021 (backup)
                                theme.upper(),              # EXISTING (backup)
                                "layer0"                    # default layer name (backup)
                            ]
                        elif theme.lower() == "potensi":
                            layer_names = [
                                "POTENSI_INDONESIA_16NOV2021",  # Correct layer name from PMTiles analysis
                                f"{theme.upper()}_{year}",      # POTENSI_2021 (backup)
                                f"{theme.lower()}_{year}",      # potensi_2021 (backup)
                                theme.upper(),                  # POTENSI (backup)
                                "layer0"                        # default layer name (backup)
                            ]
                        else:
                            # Fallback for unknown themes
                            layer_names = [
                                f"{theme.upper()}_{year}",
                                f"{theme.lower()}_{year}",
                                theme.upper(),
                                theme.lower(),
                                "layer0",
                                "data"
                            ]
                        
                        result = None
                        successful_layer = None
                        
                        for layer_name in layer_names:
                            try:
                                logger.debug(f"Trying layer name: {layer_name}")
                                result = subprocess.run([
                                    'tippecanoe-decode', '-l', layer_name, pmtiles_file
                                ], capture_output=True, text=True, timeout=60)
                                
                                if result.stdout and len(result.stdout) > 100:
                                    successful_layer = layer_name
                                    break
                            except Exception as e:
                                logger.debug(f"Layer {layer_name} failed: {e}")
                                continue
                        
                        if successful_layer:
                            logger.info(f"Found working layer name: {successful_layer}")
                        
                        if result.stdout and len(result.stdout) > 100:
                            try:
                                import json
                                json_data = json.loads(result.stdout)
                                if json_data.get('type') == 'FeatureCollection':
                                    features = json_data.get('features', [])
                                    logger.info(f"tippecanoe-decode layer extraction found {len(features)} features")
                                    
                                    # Check if features have valid geometries
                                    valid_features = 0
                                    for feature in features[:10]:  # Check first 10 features
                                        geom = feature.get('geometry')
                                        if geom and geom.get('type') and geom.get('coordinates'):
                                            valid_features += 1
                                    
                                    logger.info(f"Sample check: {valid_features}/10 features have valid geometries")
                                    
                                    if len(features) > 0 and valid_features > 0:
                                        with open(geojson_file, 'w') as f:
                                            f.write(result.stdout)
                                        
                                        if os.path.exists(geojson_file) and os.path.getsize(geojson_file) > 0:
                                            logger.info(f"✅ tippecanoe-decode layer extraction successful")
                                            conversion_success = True
                                    else:
                                        logger.error(f"Layer extraction produced {len(features)} features but no valid geometries")
                                else:
                                    logger.error(f"Layer extraction output is not valid GeoJSON FeatureCollection")
                            except json.JSONDecodeError:
                                logger.error(f"Layer extraction output is not valid JSON")
                        else:
                            logger.warning(f"Layer extraction produced no output")
                            
                    except Exception as e:
                        logger.error(f"tippecanoe-decode layer extraction error: {e}")
                
                # Method 3: Try ogr2ogr with PMTiles support
                if not conversion_success:
                    try:
                        logger.info(f"Trying ogr2ogr PMTiles conversion for {theme}...")
                        
                        # Try different ogr2ogr approaches for PMTiles
                        ogr_attempts = [
                            # Standard conversion
                            ['ogr2ogr', '-f', 'GeoJSON', geojson_file, pmtiles_file],
                            # Try with layer specification
                            ['ogr2ogr', '-f', 'GeoJSON', geojson_file, pmtiles_file, f"{theme.upper()}_{year}"],
                            # Try with MVT approach
                            ['ogr2ogr', '-f', 'GeoJSON', '-oo', 'ZOOM_LEVEL=14', geojson_file, pmtiles_file]
                        ]
                        
                        for attempt, cmd in enumerate(ogr_attempts, 1):
                            try:
                                logger.info(f"ogr2ogr attempt {attempt} for {theme}")
                                result = subprocess.run(cmd, check=True, capture_output=True, text=True, timeout=600)
                                
                                if os.path.exists(geojson_file) and os.path.getsize(geojson_file) > 1000:
                                    if self._validate_geojson_file(geojson_file):
                                        logger.info(f"✅ ogr2ogr attempt {attempt} successful")
                                        conversion_success = True
                                        break
                                    else:
                                        logger.warning(f"ogr2ogr attempt {attempt} produced invalid GeoJSON")
                                        if os.path.exists(geojson_file):
                                            os.remove(geojson_file)
                                else:
                                    logger.warning(f"ogr2ogr attempt {attempt} produced no/small output")
                                    
                            except subprocess.CalledProcessError as e:
                                logger.warning(f"ogr2ogr attempt {attempt} failed: {e}")
                                if os.path.exists(geojson_file):
                                    os.remove(geojson_file)
                                continue
                                
                    except Exception as e:
                        logger.error(f"ogr2ogr PMTiles conversion error: {e}")
                
                # Method 4: Try Python pmtiles library (if available)
                if not conversion_success:
                    try:
                        logger.info(f"Trying Python pmtiles library for {theme}...")
                        
                        # Try to use pmtiles Python library
                        try:
                            import pmtiles
                            from pmtiles.reader import Reader
                            
                            # Read PMTiles file
                            with open(pmtiles_file, 'rb') as f:
                                reader = Reader(f)
                                
                                # Get all tiles at zoom level 0
                                features = []
                                for tile_id, tile_data in reader.all_tiles():
                                    # Process tile data (this is a simplified approach)
                                    pass
                                
                                # Create GeoJSON structure
                                geojson_data = {
                                    "type": "FeatureCollection",
                                    "features": features
                                }
                                
                                with open(geojson_file, 'w') as out_f:
                                    import json
                                    json.dump(geojson_data, out_f)
                                
                                if os.path.exists(geojson_file) and os.path.getsize(geojson_file) > 100:
                                    logger.info(f"✅ Python pmtiles library successful")
                                    conversion_success = True
                                    
                        except ImportError:
                            logger.warning(f"Python pmtiles library not available")
                        except Exception as e:
                            logger.warning(f"Python pmtiles library failed: {e}")
                            
                    except Exception as e:
                        logger.error(f"Python pmtiles processing error: {e}")
                
                # Method 4: Try ogr2ogr direct conversion (if PMTiles driver available)
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
                
                # Method 5: Last resort - try tile-proxy method
                if not conversion_success:
                    try:
                        logger.info(f"Trying tile-proxy method for {theme}...")
                        
                        # Create a simple tile server approach using tippecanoe-decode
                        # Extract all data by getting bounds and extracting systematically
                        result = subprocess.run([
                            'tippecanoe-decode', '-s', pmtiles_file
                        ], capture_output=True, text=True, timeout=60)
                        
                        if result.returncode == 0 and result.stdout:
                            # Parse the stats to get bounds
                            stats_lines = result.stdout.strip().split('\n')
                            logger.info(f"PMTiles stats: {stats_lines[:3]}...")  # Show first few lines
                            
                            # Try extracting with no layer filter
                            result = subprocess.run([
                                'tippecanoe-decode', '--no-feature-limit', pmtiles_file
                            ], capture_output=True, text=True, timeout=300)
                            
                            if result.stdout and len(result.stdout) > 100:
                                # Check if output contains valid features
                                if '"type":"Feature"' in result.stdout or '"type": "Feature"' in result.stdout:
                                    # Try to extract just the features part
                                    lines = result.stdout.split('\n')
                                    geojson_lines = []
                                    in_features = False
                                    
                                    for line in lines:
                                        if '"type": "Feature"' in line or '"type":"Feature"' in line:
                                            if not geojson_lines:
                                                geojson_lines.append('{"type": "FeatureCollection", "features": [')
                                            else:
                                                geojson_lines.append(',')
                                            geojson_lines.append(line.strip())
                                            in_features = True
                                        elif in_features and (line.strip().startswith('{') or line.strip().startswith('"')):
                                            geojson_lines.append(line.strip())
                                    
                                    if geojson_lines:
                                        geojson_lines.append(']}')
                                        geojson_content = '\n'.join(geojson_lines)
                                        
                                        try:
                                            import json
                                            json.loads(geojson_content)  # Validate JSON
                                            
                                            with open(geojson_file, 'w') as f:
                                                f.write(geojson_content)
                                            
                                            if os.path.exists(geojson_file) and os.path.getsize(geojson_file) > 0:
                                                logger.info(f"✅ Tile-proxy method successful")
                                                conversion_success = True
                                        except json.JSONDecodeError:
                                            logger.warning(f"Could not create valid GeoJSON from tile-proxy method")
                        
                    except Exception as e:
                        logger.error(f"Tile-proxy method error: {e}")
                
                # Final method: Try custom conversion script
                if not conversion_success:
                    try:
                        logger.info(f"Trying custom conversion script for {theme}...")
                        
                        # Use our custom converter
                        converter_script = os.path.join(os.path.dirname(__file__), 'pmtiles_converter.py')
                        if os.path.exists(converter_script):
                            result = subprocess.run([
                                'python', converter_script, pmtiles_file, geojson_file
                            ], capture_output=True, text=True, timeout=300)
                            
                            if result.returncode == 0 and os.path.exists(geojson_file):
                                file_size = os.path.getsize(geojson_file)
                                if file_size > 100:  # Valid size check
                                    logger.info(f"✅ Custom conversion script successful")
                                    conversion_success = True
                                else:
                                    logger.warning(f"Custom script produced small file: {file_size} bytes")
                            else:
                                logger.warning(f"Custom conversion script failed: {result.stderr}")
                        else:
                            logger.warning(f"Custom converter script not found: {converter_script}")
                            
                    except Exception as e:
                        logger.error(f"Custom conversion script error: {e}")
                
                # Check if any conversion method succeeded
                if not conversion_success:
                    logger.error(f"All PMTiles conversion methods failed for {theme}")
                    
                    # Create a minimal fallback GeoJSON to prevent complete failure
                    fallback_geojson = {
                        "type": "FeatureCollection",
                        "features": [],
                        "properties": {
                            "note": f"Conversion failed for {theme} {year}, fallback empty collection created"
                        }
                    }
                    
                    try:
                        import json
                        with open(geojson_file, 'w') as f:
                            json.dump(fallback_geojson, f)
                        logger.warning(f"Created fallback empty GeoJSON for {theme}")
                    except Exception as e:
                        logger.error(f"Failed to create fallback GeoJSON: {e}")
                        raise Exception(f"All PMTiles conversion methods failed for {theme}")
                
                # Verify output file
                if not os.path.exists(geojson_file):
                    raise Exception(f"No GeoJSON output file created for {theme}")
                
                file_size = os.path.getsize(geojson_file)
                if file_size == 0:
                    raise Exception(f"Empty GeoJSON output produced for {theme}")
                
                logger.info(f"✓ Successfully converted PMTiles to GeoJSON: {geojson_file} ({file_size:,} bytes)")
                
                # Validate GeoJSON structure
                try:
                    import json
                    with open(geojson_file, 'r') as f:
                        geojson_data = json.load(f)
                    
                    if geojson_data.get('type') != 'FeatureCollection':
                        logger.warning(f"GeoJSON is not a FeatureCollection for {theme}")
                    else:
                        features_count = len(geojson_data.get('features', []))
                        logger.info(f"GeoJSON validation: {features_count} features for {theme}")
                        
                except json.JSONDecodeError as e:
                    logger.error(f"Invalid JSON produced for {theme}: {e}")
                    raise Exception(f"Invalid GeoJSON output produced for {theme}")
                except Exception as e:
                    logger.warning(f"Could not validate GeoJSON for {theme}: {e}")
                
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
        
        # Debug: Check overall geometry state
        total_null = gdf.geometry.isna().sum()
        total_empty = gdf.geometry.is_empty.sum() if total_null < len(gdf) else 0
        logger.info(f"Geometry overview: {len(gdf)} total, {total_null} null, {total_empty} empty")
        
        # If all geometries are null/empty, this is a fundamental data issue
        if total_null + total_empty == len(gdf):
            logger.error(f"All geometries in {theme} data are null or empty - GeoJSON conversion failed")
            raise Exception(f"All geometries in {theme} data are null or empty")
        
        # Filter geometries - try to keep or convert to polygon-like geometries
        original_count = len(gdf)
        valid_geom_types = ['Polygon', 'MultiPolygon']

        # Get actual geometry types for debugging
        try:
            actual_types = gdf[gdf.geometry.notna() & (~gdf.geometry.is_empty)].geometry.geom_type.value_counts()
            logger.info(f"Actual geometry types in data: {dict(actual_types)}")
        except Exception as e:
            logger.warning(f"Failed to get geometry types: {e}")

        # Split polygons and non-polygons (only from valid geometries)
        valid_mask = gdf.geometry.notna() & (~gdf.geometry.is_empty)
        valid_gdf = gdf[valid_mask].copy()
        
        if len(valid_gdf) == 0:
            logger.error(f"No valid (non-null, non-empty) geometries found in {theme} data")
            raise Exception(f"No valid geometries found in {theme} data for year {year}")
            
        polygons_gdf = valid_gdf[valid_gdf.geometry.geom_type.isin(valid_geom_types)].copy()
        nonpolygons_gdf = valid_gdf[~valid_gdf.geometry.geom_type.isin(valid_geom_types)].copy()

        logger.info(f"Geometry types: {polygons_gdf.shape[0]} polygon(s), {nonpolygons_gdf.shape[0]} non-polygon(s) out of {original_count}")
        
        # Log detailed geometry type breakdown
        if len(nonpolygons_gdf) > 0:
            # Check for None or empty geometries first
            valid_geoms = nonpolygons_gdf.geometry.notna() & (~nonpolygons_gdf.geometry.is_empty)
            logger.info(f"Non-polygon geometries: {len(nonpolygons_gdf)} total, {valid_geoms.sum()} valid (non-null/non-empty)")
            
            if valid_geoms.sum() > 0:
                geom_type_counts = nonpolygons_gdf[valid_geoms].geometry.geom_type.value_counts()
                logger.info(f"Non-polygon breakdown: {dict(geom_type_counts)}")
            else:
                logger.warning("All non-polygon geometries are None or empty!")
                
            # Sample a few geometries for debugging
            sample_size = min(5, len(nonpolygons_gdf))
            for i in range(sample_size):
                geom = nonpolygons_gdf.iloc[i].geometry
                if geom is not None:
                    try:
                        logger.debug(f"Sample geometry {i}: type={geom.geom_type}, is_empty={geom.is_empty}, bounds={geom.bounds}")
                    except Exception as e:
                        logger.debug(f"Sample geometry {i}: error accessing properties - {e}")
                else:
                    logger.debug(f"Sample geometry {i}: None")

        # Attempt brute force conversion: buffer all non-polygon geometries
        logger.info(f"Attempting brute force conversion of {len(nonpolygons_gdf)} non-polygon geometries...")
        
        # Try multiple buffer sizes 
        buffer_sizes = [0.0001, 0.001, 0.01]
        converted_records = []
        conversion_count = 0
        
        for idx, row in nonpolygons_gdf.iterrows():
            geom = row.geometry
            success = False
            
            try:
                if geom is None or geom.is_empty:
                    continue
                    
                # Try each buffer size until one works
                for buffer_size in buffer_sizes:
                    try:
                        new_geom = geom.buffer(buffer_size)
                        
                        if (new_geom is not None and 
                            not new_geom.is_empty and 
                            hasattr(new_geom, 'geom_type') and
                            new_geom.geom_type in valid_geom_types):
                            
                            row.geometry = new_geom
                            converted_records.append(row)
                            conversion_count += 1
                            success = True
                            break
                            
                    except Exception as e:
                        logger.debug(f"Buffer size {buffer_size} failed for geometry {idx}: {e}")
                        continue
                
                # If standard buffering failed, try polygonize approach for lines
                if not success:
                    try:
                        # Try to detect if it might be a line by attempting polygonize
                        u = unary_union(geom)
                        polys = list(polygonize(u))
                        if polys:
                            new_geom = MultiPolygon(polys) if len(polys) > 1 else polys[0]
                            if (new_geom is not None and 
                                not new_geom.is_empty and 
                                hasattr(new_geom, 'geom_type') and
                                new_geom.geom_type in valid_geom_types):
                                
                                row.geometry = new_geom
                                converted_records.append(row)
                                conversion_count += 1
                                success = True
                    except Exception as e:
                        logger.debug(f"Polygonize approach failed for geometry {idx}: {e}")
                
                if not success:
                    logger.debug(f"All conversion attempts failed for geometry {idx}")
                    
            except Exception as e:
                logger.debug(f"Complete failure for geometry {idx}: {e}")
                
        logger.info(f"Brute force conversion result: {conversion_count} geometries successfully converted")

        converted_gdf = gpd.GeoDataFrame(converted_records, columns=gdf.columns) if converted_records else gpd.GeoDataFrame(columns=gdf.columns)

        # Combine original polygons and converted ones
        gdf = pd.concat([polygons_gdf, converted_gdf], ignore_index=True)

        converted_count = len(converted_gdf)
        if converted_count > 0:
            logger.info(f"✅ Successfully converted {converted_count} out of {len(nonpolygons_gdf)} non-polygon geometries into polygons")
        else:
            logger.error(f"❌ Failed to convert any of the {len(nonpolygons_gdf)} non-polygon geometries using brute force approach")

        total_valid = len(gdf)
        logger.info(f"Final result: {total_valid} total geometries ({len(polygons_gdf)} original polygons + {converted_count} converted)")
        
        if total_valid == 0:
            raise Exception(f"No valid polygon geometries found in {theme} data for year {year} after conversion attempts")

        # Additional validation - remove invalid geometries
        valid_geoms = gdf.geometry.is_valid
        if not valid_geoms.all():
            invalid_count = (~valid_geoms).sum()
            logger.warning(f"Found {invalid_count} invalid geometries, attempting to fix...")

            # Try to fix invalid geometries by buffer(0)
            try:
                gdf.loc[~valid_geoms, 'geometry'] = gdf.loc[~valid_geoms, 'geometry'].buffer(0)
            except Exception as e:
                logger.warning(f"buffer(0) operation failed on some geometries: {e}")

            # Check again and remove if still invalid
            still_invalid = ~gdf.geometry.is_valid
            if still_invalid.any():
                invalid_final = still_invalid.sum()
                gdf = gdf[~still_invalid]
                logger.warning(f"Removed {invalid_final} geometries that couldn't be fixed")

        # Remove empty geometries
        non_empty = ~gdf.geometry.is_empty
        if not non_empty.all():
            empty_count = (~non_empty).sum()
            gdf = gdf[non_empty]
            logger.warning(f"Removed {empty_count} empty geometries")

        if len(gdf) == 0:
            raise Exception(f"No valid geometries remaining after filtering for {theme} data for year {year}")

        logger.info(f"Final dataset: {len(gdf)} valid features for {theme} {year}")
        
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
            
            # Validate and upload Shapefile to S3
            shp_s3_path = f"{S3_CONFIG['s3_prefix']}/pmn-result/{year}/AR_25K_PETAMANGROVE_{theme.upper()}_{year}.zip"
            
            if self._validate_shapefile(shp_zip):
                shp_size = os.path.getsize(shp_zip)
                logger.info(f"✓ Shapefile validation passed: {shp_size:,} bytes")
                
                self.s3_client.upload_file(shp_zip, S3_CONFIG['bucket'], shp_s3_path)
                logger.info(f"✓ Uploaded Shapefile: {shp_s3_path} ({shp_size:,} bytes)")
            else:
                raise Exception(f"Shapefile validation failed: {shp_zip}")
            
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
            
            # Validate and upload GDB to S3
            gdb_s3_path = f"{S3_CONFIG['s3_prefix']}/pmn-result/{year}/AR_25K_PETAMANGROVE_{theme.upper()}_{year}.gdb.zip"
            
            if self._validate_gdb(gdb_zip):
                gdb_size = os.path.getsize(gdb_zip)
                logger.info(f"✓ GDB validation passed: {gdb_size:,} bytes")
                
                self.s3_client.upload_file(gdb_zip, S3_CONFIG['bucket'], gdb_s3_path)
                logger.info(f"✓ Uploaded GDB: {gdb_s3_path} ({gdb_size:,} bytes)")
            else:
                raise Exception(f"GDB validation failed: {gdb_zip}")
            
            # Update metadata in compiler_datasets
            pmtiles_path = f"{S3_CONFIG['s3_prefix']}/layers/{theme.upper()}{year}.pmtiles"
            pmtiles_url = f"{S3_CONFIG['public_host']}/{pmtiles_path}"
            self._update_historical_metadata(year, theme, shp_zip, gdb_zip, pmtiles_url)
            
            logger.info(f"✅ Successfully completed conversion and metadata update for {theme} {year}")
            
        except Exception as e:
            logger.error(f"Failed to convert {theme} for year {year}: {e}")
            raise
        finally:
            # Cleanup
            if os.path.exists(year_temp_dir):
                import shutil
                shutil.rmtree(year_temp_dir)
    
    def _update_historical_metadata(self, year: int, theme: str, shp_zip: str, gdb_zip: str, pmtiles_url: str):
        """Update metadata in compiler_datasets for historical year"""
        try:
            logger.info(f"Updating metadata for {theme} {year}...")
            
            conn = self._get_db_connection()
            cursor = conn.cursor()
            
            # Calculate MD5 hash of GDB file
            with open(gdb_zip, 'rb') as f:
                md5_hash = hashlib.md5(f.read()).hexdigest()
            
            # Get file size
            file_size = os.path.getsize(gdb_zip)
            
            # Construct URLs
            gdb_s3_path = f"{S3_CONFIG['s3_prefix']}/pmn-result/{year}/AR_25K_PETAMANGROVE_{theme.upper()}_{year}.gdb.zip"
            shp_s3_path = f"{S3_CONFIG['s3_prefix']}/pmn-result/{year}/AR_25K_PETAMANGROVE_{theme.upper()}_{year}.zip"
            
            gdb_url = f"{S3_CONFIG['public_host']}/{gdb_s3_path}"
            shp_url = f"{S3_CONFIG['public_host']}/{shp_s3_path}"
            
            # Get row count from shapefile
            try:
                import tempfile
                with tempfile.TemporaryDirectory() as temp_dir:
                    with zipfile.ZipFile(shp_zip, 'r') as zip_ref:
                        zip_ref.extractall(temp_dir)
                    
                    shp_file = None
                    for file in os.listdir(temp_dir):
                        if file.endswith('.shp'):
                            shp_file = os.path.join(temp_dir, file)
                            break
                    
                    if shp_file:
                        gdf = gpd.read_file(shp_file)
                        row_count = len(gdf)
                    else:
                        row_count = 0
            except Exception as e:
                logger.warning(f"Could not get row count from shapefile: {e}")
                row_count = 0
            
            # Update or insert metadata
            title = f"Peta {'Eksisting' if theme == 'existing' else 'Potensi'} Mangrove {year}"
            
            # Check if record exists
            cursor.execute("SELECT id FROM pmn.compiler_datasets WHERE title = %s", (title,))
            existing_record = cursor.fetchone()
            
            if existing_record:
                # Update existing record
                cursor.execute("""
                    UPDATE pmn.compiler_datasets 
                    SET rows = %s, md5 = %s, date_created = %s, size = %s,
                        gdb_url = %s, shp_url = %s, map_url = %s, process = %s
                    WHERE title = %s
                """, (
                    row_count, md5_hash, datetime.now(), file_size,
                    gdb_url, shp_url, pmtiles_url, False, title
                ))
                logger.info(f"✓ Updated metadata for {theme} {year}: {row_count} rows, {file_size} bytes")
            else:
                # Insert new record
                cursor.execute("""
                    INSERT INTO pmn.compiler_datasets 
                    (title, rows, md5, date_created, size, gdb_url, shp_url, map_url, process)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    title, row_count, md5_hash, datetime.now(), file_size,
                    gdb_url, shp_url, pmtiles_url, False
                ))
                logger.info(f"✓ Inserted metadata for {theme} {year}: {row_count} rows, {file_size} bytes")
            
            conn.commit()
            cursor.close()
            conn.close()
            
        except Exception as e:
            logger.error(f"Failed to update metadata for {theme} {year}: {e}")
            logger.error(traceback.format_exc())
            # Don't raise - metadata update failure shouldn't stop the process

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
            
            # Step 5: Clean S3 files
            self.step_5_clean_s3_files()
            
            # Step 6: Delete old PMTiles
            self.step_6_delete_old_pmtiles()
            
            # Step 7: Export to GeoJSON
            geojson_files = self.step_7_export_geojson()
            
            # Step 8: Convert formats
            converted_files = self.step_8_convert_formats(geojson_files)
            
            # Step 9: Generate PMTiles (Nasional)
            pmtiles_urls = self.step_9_generate_pmtiles(geojson_files)
            
            # Step 9B: Generate PMTiles per BPDAS
            pmtiles_bpdas_urls = self.step_9b_generate_pmtiles_per_bpdas()
            
            # Step 9C: Register layers to geoportal.layers
            self.step_9c_register_geoportal_layers(pmtiles_urls, pmtiles_bpdas_urls)
            
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

def check_year_files(year: int) -> Dict[str, bool]:
    """Standalone function to check which files exist for a specific year"""
    try:
        # Initialize temporary compiler instance just for file checking
        temp_compiler = PMNCompiler("temp_check", datetime.now().year)
        return temp_compiler._check_year_files_exist(year)
    except Exception as e:
        logger.error(f"Failed to check files for year {year}: {e}")
        return {}

def process_historical_years(start_year: int = 2021, end_year: int = None, force_regenerate: bool = False):
    """Standalone function to process historical years"""
    if end_year is None:
        end_year = datetime.now().year - 1
    
    logger.info("=" * 60)
    logger.info(f"PROCESSING HISTORICAL YEARS ({start_year}-{end_year})")
    logger.info("=" * 60)
    
    # Initialize temporary compiler instance for processing
    temp_compiler = PMNCompiler("historical_process", datetime.now().year)
    
    try:
        for year in range(start_year, end_year + 1):
            logger.info(f"\n🔍 Checking files for year {year}...")
            files_status = temp_compiler._check_year_files_exist(year)
            
            # Log current status
            logger.info(f"Files status for {year}:")
            for file_type, exists in files_status.items():
                status = "✅" if exists else "❌"
                logger.info(f"  {status} {file_type}")
            
            # Check if all files exist
            all_files_exist = (
                files_status.get('pmtiles_existing', False) and files_status.get('pmtiles_potensi', False) and
                files_status.get('shp_existing', False) and files_status.get('shp_potensi', False) and
                files_status.get('gdb_existing', False) and files_status.get('gdb_potensi', False)
            )
            
            if all_files_exist and not force_regenerate:
                logger.info(f"✅ All files exist for year {year}, skipping...")
                continue
            
            # Check PMTiles availability (at least one must exist)
            pmtiles_existing_exists = files_status.get('pmtiles_existing', False)
            pmtiles_potensi_exists = files_status.get('pmtiles_potensi', False)
            any_pmtiles_exist = pmtiles_existing_exists or pmtiles_potensi_exists
            
            # Check Shapefile/GDB status
            shp_gdb_missing = not (
                files_status.get('shp_existing', False) and files_status.get('shp_potensi', False) and
                files_status.get('gdb_existing', False) and files_status.get('gdb_potensi', False)
            )
            
            # Determine if we need to process this year
            need_processing = False
            processing_reason = ""
            
            if force_regenerate:
                need_processing = True
                processing_reason = "Forced regeneration"
            elif any_pmtiles_exist and shp_gdb_missing:
                need_processing = True
                if pmtiles_existing_exists and pmtiles_potensi_exists:
                    processing_reason = "Both PMTiles exist, converting to Shapefile/GDB"
                elif pmtiles_existing_exists:
                    processing_reason = "Only EXISTING PMTiles available, converting"
                elif pmtiles_potensi_exists:
                    processing_reason = "Only POTENSI PMTiles available, converting"
            
            if need_processing:
                logger.info(f"🔄 {processing_reason} for year {year}...")
                try:
                    temp_compiler._convert_pmtiles_to_formats(year)
                    logger.info(f"✅ Successfully processed files for year {year}")
                except Exception as e:
                    logger.error(f"❌ Failed to process files for year {year}: {e}")
                    import traceback
                    logger.error(traceback.format_exc())
                    continue
            else:
                if not any_pmtiles_exist:
                    logger.warning(f"⚠️  Year {year} has no PMTiles, skipping (requires full processing)")
                else:
                    logger.info(f"ℹ️  Year {year} already complete")
        
        logger.info("\n" + "=" * 60)
        logger.info("HISTORICAL YEARS PROCESSING COMPLETED")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"Historical years processing failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise
    finally:
        # Cleanup temp compiler
        temp_compiler.cleanup()

def main():
    """Main function"""
    if len(sys.argv) < 3:
        print("Usage:")
        print("  python compile_pmn.py <process_id> <year>                     # Run main compilation")
        print("  python compile_pmn.py historical <start_year> [end_year]      # Process historical years")
        print("  python compile_pmn.py check <year>                           # Check files for specific year")
        sys.exit(1)
    
    command = sys.argv[1]
    
    if command == "historical":
        start_year = int(sys.argv[2])
        end_year = int(sys.argv[3]) if len(sys.argv) > 3 else None
        force_regenerate = "--force" in sys.argv
        process_historical_years(start_year, end_year, force_regenerate)
    elif command == "check":
        year = int(sys.argv[2])
        files_status = check_year_files(year)
        print(f"\nFiles status for year {year}:")
        for file_type, exists in files_status.items():
            status = "✅" if exists else "❌"
            print(f"  {status} {file_type}")
    else:
        # Original main compilation
        process_id = sys.argv[1]
        year = int(sys.argv[2])
        
        compiler = PMNCompiler(process_id, year)
        try:
            compiler.run()
        finally:
            compiler.cleanup()


if __name__ == "__main__":
    main()