from scraper import OFACPenaltyScraper
import sqlite3
from datetime import datetime

def repair_2024_ids():
    """
    Repairs IDs for 2024 records that were incorrectly stored with 2025.
    """
    conn = sqlite3.connect('ofac_penalties.db')
    cursor = conn.cursor()
    
    try:
        # Find all records from 2024 that might have incorrect IDs
        cursor.execute("""
            SELECT id, date
            FROM penalties
            WHERE date >= '2024-01-01' 
            AND date < '2025-01-01'
            AND id LIKE '%-2025'
        """)
        records = cursor.fetchall()
        
        count = 0
        for record in records:
            old_id, date = record
            index = old_id.split('-')[0]  # Get the index part
            new_id = f"{index}-2024"      # Create the correct ID
            
            # Update the record
            cursor.execute("""
                UPDATE penalties 
                SET id = ? 
                WHERE id = ?
            """, (new_id, old_id))
            
            # Update the linked_penalties in the PDFs table
            cursor.execute("""
                UPDATE penalties_pdfs
                SET linked_penalties = REPLACE(linked_penalties, ?, ?)
                WHERE linked_penalties LIKE ?
            """, (old_id, new_id, f"%{old_id}%"))
            
            count += 1
        
        conn.commit()
        print(f"Successfully repaired {count} records from 2024")
        
    except Exception as e:
        conn.rollback()
        print(f"Error repairing database: {e}")
        
    finally:
        conn.close()

def erase_database():
    """
    Erases all records from both the penalties and penalties_pdfs tables.
    """
    conn = sqlite3.connect('ofac_penalties.db')
    cursor = conn.cursor()
    
    try:
        # Get current count of records
        cursor.execute("SELECT COUNT(*) FROM penalties")
        penalties_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM penalties_pdfs")
        pdfs_count = cursor.fetchone()[0]
        
        # Delete all records from both tables
        cursor.execute("DELETE FROM penalties")
        cursor.execute("DELETE FROM penalties_pdfs")
        
        conn.commit()
        print(f"Successfully erased {penalties_count} penalties and {pdfs_count} PDF records from the database")
        
    except Exception as e:
        conn.rollback()
        print(f"Error erasing database: {e}")
        
    finally:
        conn.close()

def re_scrape_all_data():
    current_year = datetime.now().year
    scraper = OFACPenaltyScraper()
    scraper.scrape_and_store(start_year=2003, end_year=current_year)

# repair_2024_ids()
# erase_database()
# re_scrape_all_data()