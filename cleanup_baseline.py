#!/usr/bin/env python3
"""
Cleanup Baseline Script
Script untuk menghapus semua table baseline dari BPDAS databases dan postgres.pmn
"""

import sys
import logging
import psycopg2
from typing import List

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
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class CleanupBaseline:
    def __init__(self, year: int, test_mode: bool = False):
        """
        Initialize CleanupBaseline
        
        Args:
            year: Year to cleanup (e.g., 2025)
            test_mode: If True, only cleanup bpdastesting database
        """
        self.year = year
        self.test_mode = test_mode
        self.bpdas_list = []
        
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

    def get_bpdas_list(self):
        """Get BPDAS list"""
        if self.test_mode:
            self.bpdas_list = ['bpdastesting']
            logger.info("TEST MODE: Only cleaning bpdastesting")
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
        
        logger.info(f"Total BPDAS to cleanup: {len(self.bpdas_list)}")

    def cleanup_pmn_tables(self):
        """Cleanup baseline tables from postgres.pmn schema"""
        logger.info("=" * 60)
        logger.info("CLEANING UP PMN BASELINE TABLES")
        logger.info("=" * 60)
        
        try:
            conn = self._get_db_connection()
            cursor = conn.cursor()
            
            tables_to_drop = [
                f'existing_{self.year}_baseline',
                f'potensi_{self.year}_baseline'
            ]
            
            for table in tables_to_drop:
                # Check if table exists
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'pmn' 
                        AND table_name = %s
                    )
                """, (table,))
                
                exists = cursor.fetchone()[0]
                
                if exists:
                    cursor.execute(f"DROP TABLE pmn.{table} CASCADE")
                    logger.info(f"  ✓ Dropped table pmn.{table}")
                else:
                    logger.info(f"  - Table pmn.{table} does not exist, skipping")
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info("PMN cleanup completed")
            
        except Exception as e:
            logger.error(f"Failed to cleanup PMN tables: {e}")
            raise

    def cleanup_bpdas_tables(self):
        """Cleanup baseline tables from all BPDAS databases"""
        logger.info("=" * 60)
        logger.info("CLEANING UP BPDAS BASELINE TABLES")
        logger.info("=" * 60)
        
        cleaned_count = 0
        skipped_count = 0
        
        for idx, bpdas_db in enumerate(self.bpdas_list, 1):
            logger.info(f"\n[{idx}/{len(self.bpdas_list)}] Cleaning {bpdas_db}...")
            
            try:
                conn = self._get_db_connection(bpdas_db)
                cursor = conn.cursor()
                
                tables_to_drop = [
                    f'existing_{self.year}_baseline',
                    f'potensi_{self.year}_baseline'
                ]
                
                dropped_any = False
                for table in tables_to_drop:
                    # Check if table exists
                    cursor.execute("""
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables 
                            WHERE table_schema = 'public' 
                            AND table_name = %s
                        )
                    """, (table,))
                    
                    exists = cursor.fetchone()[0]
                    
                    if exists:
                        cursor.execute(f"DROP TABLE public.{table} CASCADE")
                        logger.info(f"    ✓ Dropped table public.{table}")
                        dropped_any = True
                    else:
                        logger.info(f"    - Table public.{table} does not exist")
                
                conn.commit()
                cursor.close()
                conn.close()
                
                if dropped_any:
                    cleaned_count += 1
                    logger.info(f"  ✓ {bpdas_db} cleaned successfully")
                else:
                    logger.info(f"  - {bpdas_db} had no baseline tables")
                
            except psycopg2.OperationalError as e:
                logger.warning(f"  ✗ Cannot connect to {bpdas_db}: {e}")
                skipped_count += 1
                continue
            except Exception as e:
                logger.error(f"  ✗ Error cleaning {bpdas_db}: {e}")
                skipped_count += 1
                continue
        
        logger.info("=" * 60)
        logger.info("CLEANUP SUMMARY:")
        logger.info(f"  Total BPDAS cleaned: {cleaned_count}")
        logger.info(f"  Total BPDAS skipped: {skipped_count}")
        logger.info("=" * 60)

    def run(self):
        """Run the cleanup process"""
        try:
            logger.info("=" * 80)
            logger.info("STARTING BASELINE CLEANUP PROCESS")
            logger.info(f"Year: {self.year}")
            logger.info(f"Test Mode: {self.test_mode}")
            logger.info("=" * 80)
            
            # Confirm before proceeding
            if not self.test_mode:
                print("\n⚠️  WARNING: This will delete baseline tables from ALL BPDAS databases!")
                print(f"⚠️  Tables to delete: existing_{self.year}_baseline, potensi_{self.year}_baseline")
                confirm = input("\nType 'DELETE' to confirm: ")
                
                if confirm != 'DELETE':
                    logger.info("Cleanup cancelled by user")
                    return False
            
            # Get BPDAS list
            self.get_bpdas_list()
            
            # Cleanup PMN tables
            self.cleanup_pmn_tables()
            
            # Cleanup BPDAS tables
            self.cleanup_bpdas_tables()
            
            logger.info("=" * 80)
            logger.info("✓ CLEANUP COMPLETED SUCCESSFULLY!")
            logger.info("=" * 80)
            
            return True
            
        except Exception as e:
            logger.error(f"Cleanup failed: {e}")
            return False


def main():
    """Main function"""
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python cleanup_baseline.py <year>              # Cleanup all BPDAS")
        print("  python cleanup_baseline.py <year> --test       # Cleanup bpdastesting only")
        sys.exit(1)
    
    year = int(sys.argv[1])
    test_mode = '--test' in sys.argv
    
    cleaner = CleanupBaseline(year, test_mode=test_mode)
    success = cleaner.run()
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
