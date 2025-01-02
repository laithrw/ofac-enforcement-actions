import streamlit as st
import sqlite3
from datetime import datetime, date, timedelta
import pandas as pd
import re
from typing import List, Tuple
import webbrowser
from scraper import OFACPenaltyScraper
import json
import os

class SearchType:
    EXACT = "Exact match"
    AND = "Contains all words"
    OR = "Contains any word"

def setup_page():
    st.set_page_config(
        page_title="OFAC Search",
        page_icon="🔍",
        layout="wide"
    )
    st.title("Search OFAC Public Enforcement Resolutions")
    
    # Add custom CSS to remove header anchor links
    st.markdown("""
        <style>
            [data-testid='stHeaderActionElements'] {display: none;}
            #MainMenu {visibility: hidden;}
            header {visibility: hidden;}
            .stMarkdown h1 a, .stMarkdown h2 a, .stMarkdown h3 a {display: none;}
        </style>
    """, unsafe_allow_html=True)

def connect_db() -> sqlite3.Connection:
    return sqlite3.connect("ofac_penalties.db")

def search_penalties(
    search_text: str,
    search_type: str,
    start_date: date,
    end_date: date,
    conn: sqlite3.Connection
) -> List[Tuple]:
    cursor = conn.cursor()
    
    # Base query joining penalties and penalties_pdfs tables
    query = """
        SELECT DISTINCT
            p.date, 
            p.name, 
            p.aggregate_penalties_settlements_findings, 
            p.penalties_settlements_usd_total,
            p.revision_date,
            pdf.pdf_url,
            pdf.pdf_text
        FROM penalties p
        JOIN penalties_pdfs pdf ON p.id IN (
            SELECT value 
            FROM json_each('["' || REPLACE(pdf.linked_penalties, ',', '","') || '"]')
        )
        WHERE p.date >= ? AND p.date <= ?
    """
    
    params = [start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")]
    
    # Add search conditions based on search type
    if search_text:        
        if search_type == SearchType.EXACT:
            query += " AND (LOWER(p.name) LIKE ? OR LOWER(pdf.pdf_text) LIKE ?)"
            params.extend([f"%{search_text.lower()}%", f"%{search_text.lower()}%"])
        
        elif search_type == SearchType.AND:
            words = search_text.lower().split()
            for word in words:
                query += " AND (LOWER(p.name) LIKE ? OR LOWER(pdf.pdf_text) LIKE ?)"
                params.extend([f"%{word}%", f"%{word}%"])
        
        elif search_type == SearchType.OR:
            words = search_text.lower().split()
            or_conditions = []
            for word in words:
                or_conditions.append("(LOWER(p.name) LIKE ? OR LOWER(pdf.pdf_text) LIKE ?)")
                params.extend([f"%{word}%", f"%{word}%"])
            query += f" AND ({' OR '.join(or_conditions)})"
    
    # Add ORDER BY clause to sort by date in descending order
    query += " ORDER BY p.date DESC"

    cursor.execute(query, params)
    return cursor.fetchall()

def find_excerpts(text: str, search_text: str, search_type: str) -> List[Tuple[str, int]]:
    """Find all occurrences of search text in the document"""
    if not text or not search_text:
        return []
    
    # Split text into pages
    pages = text.split("\f")
    excerpts = []
    seen_excerpts = set()  # Track unique excerpts
    
    words = search_text.lower().split()
    
    for page_num, page in enumerate(pages, 1):
        page_lower = page.lower()
        
        if search_type == SearchType.EXACT:
            # Find all occurrences of the exact phrase
            start = 0
            while True:
                index = page_lower.find(search_text.lower(), start)
                if index == -1:
                    break
                    
                # Get context for this occurrence
                excerpt = extract_context(page, page[index:index+len(search_text)], index)
                
                # Only add if this exact excerpt hasn't been seen
                excerpt_key = (excerpt, page_num)
                if excerpt_key not in seen_excerpts:
                    seen_excerpts.add(excerpt_key)
                    excerpts.append((excerpt, page_num))
                
                start = index + len(search_text)  # Move past the current match
                
        elif search_type == SearchType.AND:
            # Find occurrences where all words appear
            if all(word in page_lower for word in words):
                for word in words:
                    start = 0
                    while True:
                        index = page_lower.find(word, start)
                        if index == -1:
                            break
                            
                        excerpt = extract_context(page, page[index:index+len(word)], index)
                        excerpt_key = (excerpt, page_num)
                        if excerpt_key not in seen_excerpts:
                            seen_excerpts.add(excerpt_key)
                            excerpts.append((excerpt, page_num))
                            
                        start = index + len(word)
                
        elif search_type == SearchType.OR:
            # Find occurrences of any word
            for word in words:
                start = 0
                while True:
                    index = page_lower.find(word, start)
                    if index == -1:
                        break
                        
                    excerpt = extract_context(page, page[index:index+len(word)], index)
                    excerpt_key = (excerpt, page_num)
                    if excerpt_key not in seen_excerpts:
                        seen_excerpts.add(excerpt_key)
                        excerpts.append((excerpt, page_num))
                        
                    start = index + len(word)
    
    return excerpts

def extract_context(text: str, search_text: str, index: int, context_chars: int = 100) -> str:
    """Extract text around the search term with context"""
    if not text or not search_text:
        return ""
    
    start = max(0, index - context_chars)
    end = min(len(text), index + len(search_text) + context_chars)
    
    excerpt = text[start:end]
    if start > 0:
        excerpt = f"...{excerpt}"
    if end < len(text):
        excerpt = f"{excerpt}..."
        
    return excerpt.strip()

def check_last_update():
    """Check when the last update was performed"""
    try:
        if os.path.exists('last_update.json'):
            with open('last_update.json', 'r') as f:
                data = json.load(f)
                last_update = datetime.fromisoformat(data['last_update'])
                return last_update
        return None
    except Exception as e:
        print(f"Error reading last update time: {e}")
        return None

def save_last_update():
    """Save the current time as the last update time"""
    try:
        with open('last_update.json', 'w') as f:
            json.dump({'last_update': datetime.now().isoformat()}, f)
    except Exception as e:
        print(f"Error saving last update time: {e}")

def check_for_updates(manual_update: bool = False):
    """Check for updates if 24 hours have passed since last check"""
    last_update = check_last_update()
    current_time = datetime.now()
    
    if last_update is None or (current_time - last_update) > timedelta(hours=24) or (manual_update == True):
        current_year = current_time.year
        
        with st.spinner(f"Checking for new resolutions from {current_year}..."):
            # Create a placeholder for dynamic status updates
            status_placeholder = st.empty()
            
            # Initialize counters
            initial_count = get_penalty_count()
            
            # Run the scraper for the current year
            scraper = OFACPenaltyScraper()
            scraper.scrape_and_store(current_year, current_year)
            
            # Get the new count
            final_count = get_penalty_count()
            
            # Calculate new entries
            new_entries = final_count - initial_count
            
            if new_entries > 0:
                st.success(f"Added {new_entries} new resolution{'s' if new_entries != 1 else ''}!")
            else:
                st.info("No new resolutions found.")
            
            # Save the update time
            save_last_update()
            
            # Return the number of new entries
            return new_entries
    return 0

def get_penalty_count():
    """Get the total number of penalties in the database"""
    try:
        with sqlite3.connect("ofac_penalties.db") as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM penalties")
            return cursor.fetchone()[0]
    except Exception as e:
        print(f"Error getting penalty count: {e}")
        return 0

def get_latest_resolution_date():
    """Get the date of the most recent resolution"""
    try:
        with sqlite3.connect("ofac_penalties.db") as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT MAX(date) FROM penalties")
            result = cursor.fetchone()
            if result and result[0]:
                return datetime.strptime(result[0], '%Y-%m-%d').date()
    except Exception as e:
        print(f"Error getting latest resolution date: {e}")
    return None

def format_datetime(dt):
    """Format datetime to 'Month DD, YYYY at HH:MM AM/PM' format"""
    if isinstance(dt, datetime):
        return dt.strftime("%B %d, %Y at %I:%M %p")
    elif isinstance(dt, date):
        return dt.strftime("%B %d, %Y")
    return None

def main():
    setup_page()
    
    # Initialize session state for excerpt pagination and search pagination if not exists
    if 'excerpt_limits' not in st.session_state:
        st.session_state.excerpt_limits = {}
    if 'page_number' not in st.session_state:
        st.session_state.page_number = 1
    
    # Check for updates when the page loads
    new_entries = check_for_updates()
    
    # Sidebar for search options
    with st.sidebar:
        st.header("Search Options")
        
        # Date range selection
        min_date = date(2003, 1, 1)
        max_date = date.today()
        
        start_date = st.date_input(
            "Start Date",
            min_value=min_date,
            max_value=max_date,
            value=min_date
        )
        
        end_date = st.date_input(
            "End Date",
            min_value=min_date,
            max_value=max_date,
            value=max_date
        )
        
        # Search type selection
        search_type = st.selectbox(
            "Search Type",
            [
                SearchType.EXACT,
                SearchType.AND,
                SearchType.OR
            ]
        )
        
        # Add a visual divider
        st.divider()
        
        # Get new resolutions section
        st.header("Get New Resolutions")
        
        # Show last update time and latest resolution
        last_update = check_last_update()
        latest_date = get_latest_resolution_date()
        
        if last_update and latest_date:
            last_update_str = format_datetime(last_update)
            latest_date_str = format_datetime(latest_date)
            
            st.write(
                f"The latest resolution is from {latest_date_str}, and the last check "
                f"for new resolutions was performed on {last_update_str}. By default, "
                f"checks are performed every 24 hours. If you think a new resolution "
                f"has been added, you can manually perform another search (or reload the page if 24 hours have passed)."
            )
        
        # Manual update button
        if st.button("Check For New Resolutions"):
            new_entries = check_for_updates(manual_update=True)
            if new_entries > 0:
                st.success(f"Added {new_entries} new resolution{'s' if new_entries != 1 else ''}!")
                save_last_update()
            else:
                st.info("No new resolutions found.")
                save_last_update()

    # Main search interface
    search_text = st.text_input("Enter search terms")
    
    if search_text:
        conn = connect_db()
        results = search_penalties(search_text, search_type, start_date, end_date, conn)
        total_results = len(results)
        
        # Pagination logic
        results_per_page = 20
        total_pages = (total_results + results_per_page - 1) // results_per_page
        
        st.subheader(f"Found {total_results} results")
        
        # Create pagination controls
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            if total_pages > 1:
                pagination = st.columns(min(total_pages, 7))
                
                # Calculate page range to show
                if total_pages <= 7:
                    page_range = range(1, total_pages + 1)
                else:
                    if st.session_state.page_number <= 4:
                        page_range = list(range(1, 6)) + [0, total_pages]  # 0 represents ellipsis
                    elif st.session_state.page_number >= total_pages - 3:
                        page_range = [1, 0] + list(range(total_pages - 4, total_pages + 1))
                    else:
                        page_range = [1, 0] + list(range(st.session_state.page_number - 1, st.session_state.page_number + 2)) + [0, total_pages]
                
                # Display pagination buttons
                for i, page in enumerate(page_range):
                    if page == 0:
                        pagination[i].write("...")
                    else:
                        if pagination[i].button(
                            str(page),
                            key=f"page_{page}",
                            type="secondary" if page != st.session_state.page_number else "primary"
                        ):
                            st.session_state.page_number = page
                            st.rerun()
        
        # Slice results for current page
        start_idx = (st.session_state.page_number - 1) * results_per_page
        end_idx = start_idx + results_per_page
        page_results = results[start_idx:end_idx]
        
        # Display results for current page
        for result_idx, result in enumerate(page_results):
            date_str, name, num_penalties, amount, revision_date, pdf_url, pdf_content = result
            
            # Create a unique key for this result
            result_key = f"{date_str}_{name}_{result_idx}"
            
            # Initialize excerpt limit for this result if not exists
            if result_key not in st.session_state.excerpt_limits:
                st.session_state.excerpt_limits[result_key] = 10
            
            # Format the date string
            formatted_date = format_datetime(datetime.strptime(date_str, '%Y-%m-%d'))
            revision_info = f" (Revised: {format_datetime(revision_date)})" if revision_date else ""
            
            with st.expander(f"{formatted_date}{revision_info} - {name} - ${amount:,.2f}"):
                st.write(f"Number of Penalties: {num_penalties}")
                
                excerpts = find_excerpts(pdf_content, search_text, search_type)
                if excerpts:
                    total_excerpts = len(excerpts)
                    st.write(f"Found {total_excerpts} matching excerpt{'s' if total_excerpts != 1 else ''}")
                    
                    # Display excerpts up to the current limit
                    for i, (excerpt, _) in enumerate(excerpts):  # Ignore page_num
                        if i >= st.session_state.excerpt_limits[result_key]:
                            break
                        
                        # Replace newlines with spaces in the excerpt for the blockquote
                        formatted_excerpt = excerpt.replace('\n', '\n>')
                        
                        # Display the excerpt without page number
                        st.markdown(f">{formatted_excerpt}")
                        st.markdown("---")  # Add a separator between excerpts
                    
                    # Show "Show More" button if there are more excerpts
                    if total_excerpts > st.session_state.excerpt_limits[result_key]:
                        remaining = total_excerpts - st.session_state.excerpt_limits[result_key]
                        if st.button(f"Show {min(10, remaining)} more excerpts", key=f"more_{result_key}"):
                            st.session_state.excerpt_limits[result_key] += 10
                            st.rerun()
                
                if pdf_url:
                    st.markdown(f"[View Full PDF]({pdf_url})")
        
        conn.close()

if __name__ == "__main__":
    main()