import sqlite3
from datetime import datetime

def update_penalty_ids():
    """Update penalty IDs to reflect the correct year based on the date field."""
    try:
        conn = sqlite3.connect('ofac_penalties.db')
        cursor = conn.cursor()
        
        # Select all penalties
        cursor.execute("""
            SELECT id, date FROM penalties
        """)
        
        penalties = cursor.fetchall()
        
        for penalty in penalties:
            old_id = penalty[0]
            date_str = penalty[1]
            year = datetime.strptime(date_str, '%Y-%m-%d').year  # Extract the year from the date
            
            index = old_id.split('-')[0]  # Extract the index from the old ID
            new_id = f"{index}-{year}"  # Create the new ID based on the extracted year
            
            # Check if the new ID already exists
            cursor.execute("""
                SELECT id FROM penalties 
                WHERE id = ?
            """, (new_id,))
            existing_id = cursor.fetchone()
            
            # If the new ID exists, create a unique ID by appending a counter
            if existing_id:
                counter = 1
                while existing_id:
                    new_id = f"{index}-{year}-{counter}"  # Append counter to make it unique
                    cursor.execute("""
                        SELECT id FROM penalties 
                        WHERE id = ?
                    """, (new_id,))
                    existing_id = cursor.fetchone()
                    counter += 1
            
            # Update the penalty ID in the database only if it doesn't match the year
            if old_id != new_id:
                cursor.execute("""
                    UPDATE penalties 
                    SET id = ? 
                    WHERE id = ?
                """, (new_id, old_id))
        
        conn.commit()
        print("Updated penalty IDs to reflect the correct year based on the date field.")
        
    except Exception as e:
        print(f"Error updating penalty IDs: {e}")
        
    finally:
        conn.close()

update_penalty_ids()