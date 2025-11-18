#!/usr/bin/env python3
"""
Pemutakhiran Baseline Script
Script untuk 'mengawetkan' data QC yang sudah lolos validasi ke table baseline
di masing-masing BPDAS dan postgres.pmn untuk tahun berjalan.
"""

import sys
import os
import logging
import psycopg2
import psycopg2.extras
import json
import traceback
from datetime import datetime
from typing import List, Dict, Any, Optional

# Konfigurasi Database
DB_CONFIG = {
    'host': '52.74.112.75',
    'port': 5432,
    'user': 'pg',
    'password': '~nagha2025yasha@~'
}

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pemutakhiran_baseline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class PemutakhiranBaseline:
    def __init__(self, state_id: int, test_mode: bool = False):
        """
        Initialize PemutakhiranBaseline
        
        Args:
            state_id: ID dari pmn.state table
            test_mode: If True, only process bpdastesting database
        """
        self.state_id = state_id
        self.test_mode = test_mode
        self.year = None
        self.bpdas_list = []
        self.total_bpdas = 0
        self.processed_bpdas = 0
        self.qc_completed = 0
        
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

    def _update_state_progress(self, percentage: int, qc_completed: int = None, bpdas_complete: int = None, completed: bool = False):
        """Update progress in pmn.state table (fase2)"""
        try:
            conn = self._get_db_connection()
            cursor = conn.cursor()
            
            # Get current state
            cursor.execute("SELECT state FROM pmn.state WHERE id = %s", (self.state_id,))
            result = cursor.fetchone()
            
            if not result:
                logger.error(f"State ID {self.state_id} not found")
                return
            
            state_data = result[0]
            
            # Update fase2 metrics
            if 'fase2' not in state_data:
                state_data['fase2'] = {}
            
            if 'metrics' not in state_data['fase2']:
                state_data['fase2']['metrics'] = {}
            
            if qc_completed is not None:
                state_data['fase2']['metrics']['qc_completed'] = qc_completed
            
            if bpdas_complete is not None:
                state_data['fase2']['metrics']['bpdas_complete'] = bpdas_complete
            
            if completed:
                state_data['fase2']['completed'] = True
                state_data['fase2']['status'] = True
            
            # Update database
            cursor.execute("""
                UPDATE pmn.state 
                SET state = %s, percentage = %s
                WHERE id = %s
            """, (json.dumps(state_data), percentage, self.state_id))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"Progress updated: {percentage}% - QC: {qc_completed}, BPDAS: {bpdas_complete}")
            
        except Exception as e:
            logger.error(f"Failed to update state progress: {e}")
            raise

    def step_1_get_year_and_bpdas(self) -> bool:
        """Step 1: Get year from state and BPDAS list"""
        logger.info("=" * 60)
        logger.info("Step 1: Getting year and BPDAS list...")
        logger.info("=" * 60)
        
        try:
            conn = self._get_db_connection()
            cursor = conn.cursor()
            
            # Get year from state
            cursor.execute("SELECT tahun FROM pmn.state WHERE id = %s", (self.state_id,))
            result = cursor.fetchone()
            
            if not result:
                logger.error(f"State ID {self.state_id} not found")
                return False
            
            self.year = result[0]
            logger.info(f"Year: {self.year}")
            
            cursor.close()
            conn.close()
            
            # Get BPDAS list
            if self.test_mode:
                self.bpdas_list = ['bpdastesting']
                logger.info("TEST MODE: Only processing bpdastesting")
            else:
                self.bpdas_list = [
                    'agamkuantan', 'akemalamo', 'asahanbarumun', 
                    'barito', 'batanghari', 'baturusacerucuk', 'benainnoelmina', 
                    'bonelimboto', 'brantassampean', 'cimanukcitanduy', 'citarumciliwung', 
                    'dodokanmoyosari', 'indragirirokan', 'jeneberangsaddang', 'kahayan', 'kapuas', 
                    'karama', 'ketahun', 'konaweha', 'kruengaceh', 'mahakamberau', 'memberamo', 'musi', 
                    'paluposo', 'pemalijratun', 'remuransiki', 'seijangduriangkang', 'serayuopakprogo', 
                    'solo', 'tondano', 'undaanyar', 'waehapubatumerah', 'wampuseiular', 'wayseputihwaysekampung'
                ]
            
            self.total_bpdas = len(self.bpdas_list)
            logger.info(f"Total BPDAS to process: {self.total_bpdas}")
            
            self._update_state_progress(5, qc_completed=0, bpdas_complete=0)
            
            return True
            
        except Exception as e:
            logger.error(f"Step 1 failed: {e}")
            logger.error(traceback.format_exc())
            return False

    def step_2_create_pmn_baseline_tables(self):
        """Step 2: Create baseline tables in postgres.pmn schema"""
        logger.info("=" * 60)
        logger.info("Step 2: Creating baseline tables in pmn schema...")
        logger.info("=" * 60)
        
        try:
            conn = self._get_db_connection()
            cursor = conn.cursor()
            
            # Create existing baseline table
            existing_table = f'existing_{self.year}_baseline'
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS pmn.{existing_table} (
                    ogc_fid SERIAL PRIMARY KEY,
                    geometry GEOMETRY(MultiPolygon, 4326),
                    bpdas VARCHAR(255)
                )
            """)
            logger.info(f"✓ Table pmn.{existing_table} created/verified")
            
            # Create potensi baseline table
            potensi_table = f'potensi_{self.year}_baseline'
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS pmn.{potensi_table} (
                    ogc_fid SERIAL PRIMARY KEY,
                    geometry GEOMETRY(MultiPolygon, 4326),
                    bpdas VARCHAR(255)
                )
            """)
            logger.info(f"✓ Table pmn.{potensi_table} created/verified")
            
            # Create indexes for better performance
            cursor.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_{existing_table}_geom 
                ON pmn.{existing_table} USING GIST (geometry)
            """)
            
            cursor.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_{potensi_table}_geom 
                ON pmn.{potensi_table} USING GIST (geometry)
            """)
            
            conn.commit()
            cursor.close()
            conn.close()
            
            self._update_state_progress(10, qc_completed=0, bpdas_complete=0)
            logger.info("Step 2 completed: PMN baseline tables created")
            
        except Exception as e:
            logger.error(f"Step 2 failed: {e}")
            raise

    def _check_qc_status_all_true(self, qcstatus: List[Dict]) -> bool:
        """
        Check if all QC status items are true (except Remarks which can be any value)
        
        Args:
            qcstatus: List of QC status dictionaries
            
        Returns:
            True if all non-Remarks items have value='true'
        """
        if not qcstatus or not isinstance(qcstatus, list):
            return False
        
        for item in qcstatus:
            if not isinstance(item, dict):
                continue
            
            title = item.get('title', '')
            value = item.get('value', '')
            
            # Skip Remarks - it can have any value
            if 'remark' in title.lower():
                continue
            
            # All other items must be 'true'
            if value != 'true':
                return False
        
        return True

    def step_3_process_bpdas_databases(self):
        """Step 3: Process each BPDAS database and create/populate baseline tables"""
        logger.info("=" * 60)
        logger.info("Step 3: Processing BPDAS databases...")
        logger.info("=" * 60)
        
        try:
            pmn_conn = self._get_db_connection()
            pmn_cursor = pmn_conn.cursor()
            
            for idx, bpdas_db in enumerate(self.bpdas_list, 1):
                logger.info(f"\n[{idx}/{self.total_bpdas}] Processing BPDAS: {bpdas_db}")
                
                try:
                    # Connect to BPDAS database
                    bpdas_conn = self._get_db_connection(bpdas_db)
                    bpdas_cursor = bpdas_conn.cursor()
                    
                    # Check if source QC tables exist
                    bpdas_cursor.execute("""
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
                    """, (f'existing_{self.year}_qc', f'potensi_{self.year}_qc'))
                    
                    result = bpdas_cursor.fetchone()
                    existing_qc_exists = result[0]
                    potensi_qc_exists = result[1]
                    
                    if not existing_qc_exists and not potensi_qc_exists:
                        logger.warning(f"  ⚠ No QC tables found in {bpdas_db}, skipping...")
                        bpdas_cursor.close()
                        bpdas_conn.close()
                        continue
                    
                    # Process EXISTING baseline
                    if existing_qc_exists:
                        self._process_baseline_for_theme(
                            bpdas_db, 'existing', bpdas_cursor, pmn_cursor
                        )
                    
                    # Process POTENSI baseline
                    if potensi_qc_exists:
                        self._process_baseline_for_theme(
                            bpdas_db, 'potensi', bpdas_cursor, pmn_cursor
                        )
                    
                    bpdas_cursor.close()
                    bpdas_conn.close()
                    
                    # Update progress
                    self.processed_bpdas += 1
                    progress = 10 + int((self.processed_bpdas / self.total_bpdas) * 85)
                    self._update_state_progress(
                        progress, 
                        qc_completed=self.qc_completed, 
                        bpdas_complete=self.processed_bpdas
                    )
                    
                    logger.info(f"  ✓ {bpdas_db} processed successfully")
                    
                except psycopg2.OperationalError as e:
                    logger.warning(f"  ✗ Cannot connect to {bpdas_db}: {e}")
                    continue
                except Exception as e:
                    logger.error(f"  ✗ Error processing {bpdas_db}: {e}")
                    logger.error(traceback.format_exc())
                    continue
            
            # Final commit to PMN database
            pmn_conn.commit()
            pmn_cursor.close()
            pmn_conn.close()
            
            logger.info("=" * 60)
            logger.info("PROCESSING SUMMARY:")
            logger.info(f"  Total BPDAS processed: {self.processed_bpdas}/{self.total_bpdas}")
            logger.info(f"  Total QC data copied: {self.qc_completed}")
            logger.info("=" * 60)
            
            self._update_state_progress(95, qc_completed=self.qc_completed, bpdas_complete=self.processed_bpdas)
            logger.info("Step 3 completed: BPDAS processing finished")
            
        except Exception as e:
            logger.error(f"Step 3 failed: {e}")
            raise

    def _process_baseline_for_theme(self, bpdas_db: str, theme: str, bpdas_cursor, pmn_cursor):
        """Process baseline for a specific theme (existing/potensi)"""
        logger.info(f"    Processing {theme} baseline...")
        
        try:
            qc_table = f'{theme}_{self.year}_qc'
            baseline_table = f'{theme}_{self.year}_baseline'
            
            # 1. Check if baseline table exists in BPDAS database
            bpdas_cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = %s
                )
            """, (baseline_table,))
            
            baseline_exists = bpdas_cursor.fetchone()[0]
            
            if baseline_exists:
                logger.info(f"      ✓ Table {baseline_table} already exists in {bpdas_db}, skipping table creation")
            else:
                # Create baseline table in BPDAS database (geometry + bpdas fields)
                bpdas_cursor.execute(f"""
                    CREATE TABLE public.{baseline_table} (
                        ogc_fid SERIAL PRIMARY KEY,
                        geometry GEOMETRY(MultiPolygon, 4326),
                        bpdas VARCHAR(255)
                    )
                """)
                bpdas_cursor.connection.commit()
                
                # Create index
                bpdas_cursor.execute(f"""
                    CREATE INDEX idx_{baseline_table}_geom 
                    ON public.{baseline_table} USING GIST (geometry)
                """)
                bpdas_cursor.connection.commit()
                
                logger.info(f"      ✓ Created table {baseline_table} in {bpdas_db}")
            
            # 2. Get QC data that passed all validations
            # Use DISTINCT ON to get the latest record for each geometry (removes duplicates)
            # Use ST_AsBinary to properly handle geometry data
            query = f"""
                SELECT DISTINCT ON (ST_AsText(geometry))
                    ST_AsBinary(geometry) as geom_binary,
                    qcstatus,
                    ogc_fid
                FROM public.{qc_table}
                WHERE qcstatus IS NOT NULL
                  AND geometry IS NOT NULL
                ORDER BY ST_AsText(geometry), ogc_fid DESC
            """
            
            bpdas_cursor.execute(query)
            qc_records = bpdas_cursor.fetchall()
            
            logger.info(f"      Found {len(qc_records)} unique geometries in {qc_table}")
            
            # Filter records with all QC status = true
            valid_records = []
            for record in qc_records:
                geom_binary = record[0]
                qcstatus = record[1]
                
                if self._check_qc_status_all_true(qcstatus):
                    valid_records.append(geom_binary)
            
            logger.info(f"      {len(valid_records)} geometries passed all QC validations")
            
            if len(valid_records) == 0:
                logger.info(f"      No valid QC data to insert")
                return
            
            # 3. Insert into BPDAS baseline table (check for duplicates)
            # Use ST_Force2D to remove Z dimension
            inserted_bpdas = 0
            for geom_binary in valid_records:
                # Check if geometry already exists
                bpdas_cursor.execute(f"""
                    SELECT COUNT(*) FROM public.{baseline_table}
                    WHERE ST_Equals(geometry, ST_Force2D(ST_GeomFromWKB(%s, 4326)))
                    AND bpdas = %s
                """, (psycopg2.Binary(geom_binary), bpdas_db))
                
                exists = bpdas_cursor.fetchone()[0] > 0
                
                if not exists:
                    bpdas_cursor.execute(f"""
                        INSERT INTO public.{baseline_table} (geometry, bpdas)
                        VALUES (ST_Force2D(ST_GeomFromWKB(%s, 4326)), %s)
                    """, (psycopg2.Binary(geom_binary), bpdas_db))
                    inserted_bpdas += 1
            
            bpdas_cursor.connection.commit()
            logger.info(f"      ✓ Inserted {inserted_bpdas} new records into {bpdas_db}.{baseline_table}")
            
            # 4. Insert into PMN baseline table (with bpdas field)
            # Use ST_Force2D to remove Z dimension
            inserted_pmn = 0
            for geom_binary in valid_records:
                # Check if geometry already exists in PMN
                pmn_cursor.execute(f"""
                    SELECT COUNT(*) FROM pmn.{baseline_table}
                    WHERE ST_Equals(geometry, ST_Force2D(ST_GeomFromWKB(%s, 4326)))
                    AND bpdas = %s
                """, (psycopg2.Binary(geom_binary), bpdas_db))
                
                exists = pmn_cursor.fetchone()[0] > 0
                
                if not exists:
                    pmn_cursor.execute(f"""
                        INSERT INTO pmn.{baseline_table} (geometry, bpdas)
                        VALUES (ST_Force2D(ST_GeomFromWKB(%s, 4326)), %s)
                    """, (psycopg2.Binary(geom_binary), bpdas_db))
                    inserted_pmn += 1
            
            pmn_cursor.connection.commit()
            logger.info(f"      ✓ Inserted {inserted_pmn} new records into pmn.{baseline_table}")
            
            # Update total QC completed count
            self.qc_completed += len(valid_records)
            
        except Exception as e:
            logger.error(f"      ✗ Error processing {theme} baseline: {e}")
            logger.error(traceback.format_exc())
            raise

    def step_4_finalize(self):
        """Step 4: Finalize the process"""
        logger.info("=" * 60)
        logger.info("Step 4: Finalizing...")
        logger.info("=" * 60)
        
        try:
            self._update_state_progress(
                100, 
                qc_completed=self.qc_completed, 
                bpdas_complete=self.processed_bpdas,
                completed=True
            )
            
            logger.info("Step 4 completed: Process finalized")
            logger.info("=" * 60)
            logger.info("✓ PEMUTAKHIRAN BASELINE COMPLETED SUCCESSFULLY!")
            logger.info("=" * 60)
            
        except Exception as e:
            logger.error(f"Step 4 failed: {e}")
            raise

    def run(self):
        """Run the complete pemutakhiran baseline process"""
        try:
            logger.info("=" * 80)
            logger.info("STARTING PEMUTAKHIRAN BASELINE PROCESS")
            logger.info(f"State ID: {self.state_id}")
            logger.info(f"Test Mode: {self.test_mode}")
            logger.info("=" * 80)
            
            # Step 1: Get year and BPDAS list
            if not self.step_1_get_year_and_bpdas():
                logger.error("Failed to get year and BPDAS list")
                return False
            
            # Step 2: Create PMN baseline tables
            self.step_2_create_pmn_baseline_tables()
            
            # Step 3: Process BPDAS databases
            self.step_3_process_bpdas_databases()
            
            # Step 4: Finalize
            self.step_4_finalize()
            
            return True
            
        except Exception as e:
            logger.error(f"Pemutakhiran baseline failed: {e}")
            logger.error(traceback.format_exc())
            return False


def main():
    """Main function"""
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python pemutakhiran_baseline.py <state_id>              # Run for all BPDAS")
        print("  python pemutakhiran_baseline.py <state_id> --test      # Run for bpdastesting only")
        sys.exit(1)
    
    state_id = int(sys.argv[1])
    test_mode = '--test' in sys.argv
    
    processor = PemutakhiranBaseline(state_id, test_mode=test_mode)
    success = processor.run()
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
