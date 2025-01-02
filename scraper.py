import requests
from bs4 import BeautifulSoup
import sqlite3
import PyPDF2
import io
import os
from datetime import datetime
import re  # Make sure to import the regular expression module

class OFACPenaltyScraper:
    def __init__(self):
        # Remove hardcoded year from base URL
        self.base_url = "https://ofac.treasury.gov/civil-penalties-and-enforcement-information"
        self.penalties_url = "https://ofac.treasury.gov"
        self.db_path = "ofac_penalties.db"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        self.conn = None
        self.setup_database()

    def get_db_connection(self):
        if self.conn is None:
            self.conn = sqlite3.connect(self.db_path)
        return self.conn

    def close_db_connection(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    def setup_database(self):
        conn = self.get_db_connection()
        cursor = conn.cursor()
        
        # Create penalties table with revision_date column if it does not exist
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS penalties (
                id TEXT PRIMARY KEY,
                date DATE,
                revision_date DATE,
                name TEXT,
                aggregate_penalties_settlements_findings INTEGER,
                penalties_settlements_usd_total REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create PDFs table with linked_penalties column if it does not exist
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS penalties_pdfs (
                pdf_url TEXT PRIMARY KEY,
                pdf_text TEXT,
                linked_penalties TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.commit()

    def scrape_and_store(self, start_year: int = None, end_year: int = None):
        current_year = datetime.now().year
        start_year = start_year or current_year
        end_year = end_year or current_year

        with sqlite3.connect(self.db_path) as conn:
            self.conn = conn  # Store the connection in the instance
            
            try:
                for year in range(start_year, end_year + 1):
                    # Construct the URL based on whether the year is the current year
                    if year == current_year:
                        url = self.base_url  # No year in the URL for the current year
                    else:
                        url = f"{self.base_url}/{year}-enforcement-information"
                    
                    try:
                        response = requests.get(url, headers=self.headers)
                        soup = BeautifulSoup(response.text, 'html.parser')
                        
                        table = soup.find('table', class_='usa-table')
                        if not table:
                            print(f"No table found for year {year}")
                            continue

                        # Process each row
                        rows = table.find_all('tr')[1:-1]  # Skip header row and totals row
                        for index, row in enumerate(rows):
                            cells = row.find_all(['th', 'td'])
                            if len(cells) == 4:
                                date_cell = cells[0].find('a')
                                if date_cell:
                                    date_str = date_cell.text.strip()
                                    # Strip any hidden characters
                                    date_str = date_str.encode('ascii', 'ignore').decode('ascii').strip()
                                    
                                    # Extract the main date and revision date
                                    main_date_str, revision_date_str = self.extract_dates(date_str)
                                    try:
                                        date = datetime.strptime(main_date_str, '%m/%d/%Y').date()
                                        revision_date = datetime.strptime(revision_date_str, '%m/%d/%Y').date() if revision_date_str else None
                                    except ValueError:
                                        print(f"Invalid date format: {date_str}")
                                        continue

                                    pdf_url = date_cell['href']
                                    if pdf_url.startswith('/'):
                                        pdf_url = self.penalties_url + pdf_url

                                    name = cells[1].text.strip()
                                    
                                    # Extracting the aggregate penalties
                                    penalties_text = cells[2].text.strip()
                                    penalties = self.extract_number(penalties_text)
                                    
                                    # Extracting the total amount
                                    amount_text = cells[3].text.strip()
                                    amount = self.extract_number(amount_text)

                                    # Create a unique ID based on the index and year
                                    unique_id = f"{index}-{year}"

                                    # Check for existing entry before processing PDF
                                    if not self.entry_exists(unique_id):
                                        # Download and extract PDF content
                                        pdf_text = None
                                        try:
                                            response = requests.get(pdf_url, headers=self.headers)
                                            if response.status_code == 200:
                                                pdf_text = self.extract_pdf_text(response.content)
                                        except Exception as e:
                                            print(f"Error downloading PDF: {e}")

                                        self.store_penalty(unique_id, date, revision_date, name, penalties, amount, pdf_text, pdf_url)
                                        print(f"Processed: {date} - {name}")
                                    else:
                                        print(f"Skipping duplicate ID: {unique_id}")

                    except Exception as e:
                        print(f"Error processing year {year}: {e}")

            except Exception as e:
                print(f"Error in scraping process: {e}")
                raise e
            finally:
                self.conn = None  # Clear the connection reference

    def extract_pdf_text(self, pdf_content):
        try:
            pdf_file = io.BytesIO(pdf_content)
            pdf_reader = PyPDF2.PdfReader(pdf_file)
            
            text = ""
            for page in pdf_reader.pages:
                text += page.extract_text()
            
            return text
        except Exception as e:
            print(f"Error extracting PDF text: {e}")
            return None

    def store_pdf(self, pdf_url, pdf_text, penalty_id):
        """Store PDF information and return its URL"""
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()
            
            # Check if PDF already exists
            cursor.execute("SELECT linked_penalties FROM penalties_pdfs WHERE pdf_url = ?", (pdf_url,))
            existing_pdf = cursor.fetchone()
            
            if existing_pdf:
                # Update linked_penalties by adding the new penalty_id if not already present
                linked_penalties = existing_pdf[0]
                penalties_list = linked_penalties.split(',') if linked_penalties else []
                if penalty_id not in penalties_list:
                    penalties_list.append(penalty_id)
                    new_linked_penalties = ','.join(penalties_list)
                    cursor.execute("""
                        UPDATE penalties_pdfs 
                        SET linked_penalties = ? 
                        WHERE pdf_url = ?
                    """, (new_linked_penalties, pdf_url))
                    conn.commit()
                return pdf_url
            
            # Insert new PDF with initial penalty_id
            cursor.execute("""
                INSERT INTO penalties_pdfs (pdf_url, pdf_text, linked_penalties, created_at)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            """, (pdf_url, pdf_text, penalty_id))
            
            conn.commit()
            return pdf_url
            
        except Exception as e:
            print(f"Error storing PDF: {e}")
            return None

    def store_penalty(self, unique_id, date, revision_date, name, penalties, amount, pdf_text, pdf_url):
        """Store penalty information and link it to PDF"""
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()
            
            # Store penalty
            cursor.execute("""
                INSERT OR IGNORE INTO penalties (
                    id, date, revision_date, name, aggregate_penalties_settlements_findings,
                    penalties_settlements_usd_total, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, (unique_id, date, revision_date, name, penalties, amount))
            
            # Store or update PDF with the penalty link
            self.store_pdf(pdf_url, pdf_text, unique_id)
            
            conn.commit()
            print(f"Stored: {date} - {name} - ${amount:,.2f}")
            
        except Exception as e:
            print(f"Error storing penalty: {e}")

    def entry_exists(self, unique_id):
        """Check if an entry already exists with the same unique ID"""
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT id FROM penalties 
                WHERE id = ?
            """, (unique_id,))
            
            exists = cursor.fetchone() is not None
            return exists
            
        except Exception as e:
            print(f"Error checking for existing entry: {e}")
            return False

    def print_first_entries(self, x: int):
        """Print the first X entries from the penalties database with their linked PDFs."""
        try:
            conn = self.get_db_connection()
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute("SELECT COUNT(*) FROM penalties")
            count = cursor.fetchone()[0]
            print(f"Total entries in database: {count}")
            
            cursor.execute("""
                SELECT 
                    p.id, p.date, p.name, 
                    p.aggregate_penalties_settlements_findings,
                    p.penalties_settlements_usd_total,
                    p.created_at,
                    pdf.pdf_url,
                    pdf.pdf_text,
                    pdf.linked_penalties
                FROM penalties p
                LEFT JOIN penalties_pdfs pdf ON pdf.linked_penalties LIKE '%' || p.id || '%'
                ORDER BY p.date DESC
                LIMIT ?
            """, (x,))
            
            entries = cursor.fetchall()
            
            if entries:
                print(f"\nFirst {x} entries in the database:")
                for entry in entries:
                    print("\n-------------------")
                    print(f"ID: {entry['id']}")
                    print(f"Date: {entry['date']}")
                    print(f"Name: {entry['name']}")
                    print(f"Aggregate Penalties: {entry['aggregate_penalties_settlements_findings']}")
                    print(f"Total USD: ${entry['penalties_settlements_usd_total']:,.2f}")
                    print(f"PDF Text Preview: {entry['pdf_text'][:100]}..." if entry['pdf_text'] else "PDF Text: None")
                    print(f"PDF URL: {entry['pdf_url']}")
                    print(f"Linked Penalties: {entry['linked_penalties']}")
                    print(f"Created At: {entry['created_at']}")
                    print("-------------------")
            else:
                print("No entries found in the database.")
                
        except Exception as e:
            print(f"Error retrieving entries: {e}")
            raise e
        
        finally:
            conn.close()

    def extract_number(self, text):
        """Extracts the first numeric value from a given text."""
        # Use regex to find numbers (including commas and decimals)
        match = re.search(r'[\d,]+(?:\.\d+)?', text)
        if match:
            # Remove commas for conversion to float
            return float(match.group(0).replace(',', ''))
        return 0  # Return 0 if no number is found

    def extract_dates(self, date_str):
        """Extracts the main date and revision date from the date string."""
        # Split the date string to separate the revision date if it exists
        parts = date_str.split(' (Revised ')
        main_date_str = parts[0].strip()  # The main date
        revision_date_str = parts[1].replace(')', '').strip() if len(parts) > 1 else None  # The revision date, if present
        return main_date_str, revision_date_str