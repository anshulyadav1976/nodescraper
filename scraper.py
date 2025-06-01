import requests
from bs4 import BeautifulSoup
import csv
import time
import re
import json # Added for state saving
import os   # Added for file checks (CSV header)

BASE_URL = "https://www.browsenodes.com"
START_PATH = "/amazon.co.uk"

# --- API Key Configuration ---
# Add your ScraperAPI keys to this list. The script will cycle through them.
SCRAPER_API_KEYS = [ # Replace with your first API key
    "0b8b1fb70b7079813eaddf1d33d10acf",
    "188436ab293beecfaf328805b8cc7951",
    "9241e933ec65fefe35325483335b9256",
    "9508309cd0946ffffaa22eadb7e008a5",
    "0249283608a754952ebfd6fb3403aa85",
    "542455bd7e5fd7f67d17430cf1763870",
    "1bc4f6ef8c8bbaa18c996020252e8e74" # Replace with your second API key, add more if needed
]
# Filter out placeholder keys
SCRAPER_API_KEYS = [key for key in SCRAPER_API_KEYS if key and "YOUR_" not in key]

current_api_key_index = 0 # Start with the first API key
SCRAPER_API_URL_BASE = "http://api.scraperapi.com"

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
})

# --- State and Configuration Variables ---
USE_SCRAPER_API = True  # Global flag to control ScraperAPI usage
VISITED_PATHS_FILE = "visited_paths.json"
COMPLETED_TOP_LEVEL_FILE = "completed_top_level.txt"
CSV_FILE_NAME = "amazon_co_uk_leaf_nodes.csv"

# --- Globals for CSV writing ---
csv_writer_global = None
csv_file_object_global = None

def load_set_from_json(filepath):
    """Loads a set from a JSON file."""
    if os.path.exists(filepath):
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                return set(json.load(f))
        except (json.JSONDecodeError, IOError) as e:
            print(f"Error loading JSON from {filepath}: {e}. Starting with an empty set.")
            return set()
    return set()

def save_set_to_json(data_set, filepath):
    """Saves a set to a JSON file."""
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(list(data_set), f, indent=2)
    except IOError as e:
        print(f"Error saving JSON to {filepath}: {e}")

def load_set_from_lines(filepath):
    """Loads a set from a text file (one item per line)."""
    if os.path.exists(filepath):
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                return set(line.strip() for line in f if line.strip())
        except IOError as e:
            print(f"Error loading lines from {filepath}: {e}. Starting with an empty set.")
            return set()
    return set()

def append_line_to_file(line, filepath):
    """Appends a line to a text file."""
    try:
        with open(filepath, 'a', encoding='utf-8') as f:
            f.write(line + '\n')
    except IOError as e:
        print(f"Error appending to {filepath}: {e}")

def build_scraper_api_url(original_url, api_key):
    return f"{SCRAPER_API_URL_BASE}?api_key={api_key}&url={original_url}"

def parse_soup_from_response(response_content):
    return BeautifulSoup(response_content.decode('utf-8', 'replace'), "html.parser")

def get_soup(url_to_scrape):
    global current_api_key_index
    num_keys = len(SCRAPER_API_KEYS)

    # Attempt with API keys if any are available
    if num_keys > 0:
        start_index_for_this_attempt_cycle = current_api_key_index
        for i in range(num_keys):
            key_idx_being_tried = (start_index_for_this_attempt_cycle + i) % num_keys
            current_key_to_try = SCRAPER_API_KEYS[key_idx_being_tried]
            
            api_url = build_scraper_api_url(url_to_scrape, current_key_to_try)
            print(f"  Attempting fetch for {url_to_scrape} via API key ending ...{current_key_to_try[-5:]}")
            try:
                response = SESSION.get(api_url, timeout=60)
                response.raise_for_status() # Raises HTTPError for bad responses (4xx or 5xx)
                current_api_key_index = key_idx_being_tried # Key worked, set it as current for next overall op
                return parse_soup_from_response(response.content)
            except requests.exceptions.HTTPError as e:
                if e.response is not None and e.response.url.startswith(SCRAPER_API_URL_BASE) and \
                   e.response.status_code in [401, 403, 429]: # API key specific errors
                    print(f"  WARN: API key ...{current_key_to_try[-5:]} failed for {url_to_scrape} with status {e.response.status_code}. Trying next option.")
                    # Continue to the next key in the loop for *this URL*
                else:
                    # A different HTTP error occurred (e.g., 404 from target site, 500 from API not related to key auth)
                    print(f"  HTTPError fetching {url_to_scrape} (via API key ...{current_key_to_try[-5:]}): {e}. Not retrying with other keys for this URL.")
                    return None # Fail for this URL, don't try other keys or direct for this specific non-API-key error
            except requests.exceptions.RequestException as e: # Broader network errors like ConnectionError, Timeout
                print(f"  RequestException for {url_to_scrape} (via API key ...{current_key_to_try[-5:]}): {e}. Trying next option.")
                # Continue to the next key for this URL
            except Exception as e:
                print(f"  Unexpected error for {url_to_scrape} (via API key ...{current_key_to_try[-5:]}): {e}. Trying next option.")
                # Continue to the next key for this URL
        
        # If all keys were tried and failed for this URL, reset current_api_key_index for the next call to get_soup
        print(f"  INFO: All {num_keys} API keys failed for {url_to_scrape}.")
        current_api_key_index = 0 # Reset for the *next* get_soup call to start with the first key

    # Attempt direct connection if no API keys or all failed
    if num_keys == 0:
        print(f"  INFO: No API keys configured. Attempting direct connection for {url_to_scrape}.")
    else:
        print(f"  INFO: Attempting direct connection for {url_to_scrape} as a last resort.")
    
    try:
        response = SESSION.get(url_to_scrape, timeout=60)
        response.raise_for_status()
        return parse_soup_from_response(response.content)
    except requests.exceptions.RequestException as e:
        print(f"  Error fetching {url_to_scrape} (direct): {e}")
        return None
    except Exception as e:
        print(f"  An unexpected error occurred while fetching {url_to_scrape} (direct): {e}")
        return None

def extract_node_id_from_path(path_segment):
    """Extracts Node ID from a URL path segment."""
    if not path_segment: return None
    parts = path_segment.split('/')
    for part in reversed(parts):
        if '.html' in part:
            return part.replace('.html', '')
    if parts and parts[-1].isdigit():
        return parts[-1]
    # print(f"WARN: Could not extract Node ID from path: {path_segment}") # Can be noisy
    return None

def get_child_nodes(soup, current_url_for_logging=""):
    """Extracts child node links from a BeautifulSoup object's table."""
    child_nodes = []
    if not soup:
        return child_nodes

    table = soup.find("table")
    if not table:
        return child_nodes

    is_main_page = (current_url_for_logging == BASE_URL + START_PATH)

    for i, row in enumerate(table.find_all("tr")):
        cols = row.find_all("td")
        if not cols: 
            continue

        name = ""
        node_id = ""
        path = None
        is_leaf = True 

        if len(cols) >= 2:
            name = cols[0].text.strip()
            node_id = cols[1].text.strip()
        else:
            continue 

        link_cell_idx = -1
        min_cols_for_link_cell_existence = 0

        if is_main_page:
            min_cols_for_link_cell_existence = 4 
            link_cell_idx = 3
        else:
            min_cols_for_link_cell_existence = 3 
            link_cell_idx = 2

        if len(cols) >= min_cols_for_link_cell_existence:
            browse_link_cell = cols[link_cell_idx]
            browse_link_tag = browse_link_cell.find("a")
            if browse_link_tag and browse_link_tag.has_attr("href"):
                link_text = browse_link_tag.text.strip()
                if "Browse" in link_text:
                    path = browse_link_tag["href"]
                    is_leaf = False
        
        if is_leaf:
            child_nodes.append({"name": name, "id": node_id, "path": None, "is_leaf": True})
        else:
            child_nodes.append({"name": name, "id": node_id, "path": path, "is_leaf": False})

    return child_nodes

def scrape_leaf_nodes(current_path, visited_paths_set):
    """Recursively scrapes for leaf nodes. Writes to global CSV writer as it finds them."""
    global csv_writer_global, csv_file_object_global

    full_url = BASE_URL + current_path
    if full_url in visited_paths_set:
        return # Already processed this page
    
    # Add to visited_paths_set *before* making the request
    # If request fails, it's still "visited" in the sense we tried.
    visited_paths_set.add(full_url) 

    print(f"Scraping: {full_url}")
    soup = get_soup(full_url)
    time.sleep(1) 

    if not soup:
        print(f"  Failed to get soup for {full_url}. Skipping.")
        return

    # 1. Check for explicit "is a leaf node" message
    found_leaf_message_text = None
    leaf_message_div = soup.find("div", class_="alert-info")
    if leaf_message_div and "is a leaf node. It has no child node." in leaf_message_div.text:
        found_leaf_message_text = leaf_message_div.text.strip()
    
    if not found_leaf_message_text:
        td_elements_with_colspan = soup.find_all("td", attrs={"colspan": True})
        for td_element in td_elements_with_colspan:
            if "is a leaf node. It has no child node." in td_element.text:
                found_leaf_message_text = td_element.text.strip()
                break

    if found_leaf_message_text:
        pattern = r"^(.*?) is a leaf node\. It has no child node\.$"
        match = re.search(pattern, found_leaf_message_text)
        if match:
            leaf_name = match.group(1).strip()
            leaf_id = extract_node_id_from_path(current_path)
            if leaf_name and leaf_id:
                print(f"  Found TRUE LEAF (explicit message): Name: '{leaf_name}', ID: {leaf_id}")
                if csv_writer_global:
                    csv_writer_global.writerow({"id": leaf_id, "name": leaf_name})
                    if csv_file_object_global: csv_file_object_global.flush()
                return # This path is a leaf page, no further children to process from here
            # else: print(f"  WARN: Leaf message, but no name/ID for {current_path}")
        # else: print(f"  WARN: Leaf message text '{found_leaf_message_text}' no match for {current_path}")
        return # Problem with parsing leaf message, treat as processed.

    # 2. If no explicit message, try parsing table for child categories
    child_nodes_on_page = get_child_nodes(soup, full_url)

    if child_nodes_on_page:
        for node in child_nodes_on_page:
            if node.get("is_leaf"):
                # This is a leaf item listed in a table on a non-leaf page
                print(f"  Found LEAF (table item): Name: '{node['name']}', ID: {node['id']}")
                if csv_writer_global:
                    csv_writer_global.writerow({"id": node['id'], "name": node['name']})
                    if csv_file_object_global: csv_file_object_global.flush()
            elif node["path"]:
                scrape_leaf_nodes(node["path"], visited_paths_set) # Recursive call
    elif current_path != START_PATH: # No children from table AND not main page AND no explicit leaf message
        # 3. This might be a leaf page that doesn't have explicit message OR table with items.
        h2_tag = soup.find("h2")
        page_title_name = None
        if h2_tag:
            h2_text = h2_tag.text.strip()
            if h2_text.startswith("Browse Nodes in "):
                page_title_name = h2_text.replace("Browse Nodes in ", "").strip()
        
        current_page_id = extract_node_id_from_path(current_path)
        if page_title_name and current_page_id:
            print(f"  Found TRUE LEAF (no table/msg, H2 title): Name: '{page_title_name}', ID: {current_page_id}")
            if csv_writer_global:
                csv_writer_global.writerow({"id": current_page_id, "name": page_title_name})
                if csv_file_object_global: csv_file_object_global.flush()
        # else:
            # print(f"  WARN: No explicit leaf msg, no table items, no H2 title on {full_url}. Skipping.")
    # else: (Main start page with no children - should not happen for browsenodes.com)
        # print(f"DEBUG: Reached end of scrape_leaf_nodes for {full_url}. No table children, was start page, or processed as explicit leaf.")


if __name__ == "__main__":
    print("Starting scraper with multi-API key rotation and direct fallback...")
    if not SCRAPER_API_KEYS:
        print("WARN: No ScraperAPI keys configured. Will use direct connection only.")
    else:
        print(f"INFO: Found {len(SCRAPER_API_KEYS)} ScraperAPI key(s).")

    # Load state
    visited_paths = load_set_from_json(VISITED_PATHS_FILE)
    completed_top_level_categories = load_set_from_lines(COMPLETED_TOP_LEVEL_FILE)
    
    print(f"Loaded {len(visited_paths)} visited paths.")
    print(f"Loaded {len(completed_top_level_categories)} completed top-level categories.")

    initial_soup = None
    try:
        # Setup CSV writera
        file_exists = os.path.exists(CSV_FILE_NAME)
        is_empty = (os.path.getsize(CSV_FILE_NAME) == 0) if file_exists else True
        
        csv_file_object_global = open(CSV_FILE_NAME, 'a', newline='', encoding='utf-8')
        csv_writer_global = csv.DictWriter(csv_file_object_global, fieldnames=["id", "name"])
        
        if is_empty: # Write header only if file is new/empty
            csv_writer_global.writeheader()
            if csv_file_object_global: csv_file_object_global.flush()

        print(f"Fetching initial page: {BASE_URL + START_PATH}")
        initial_soup = get_soup(BASE_URL + START_PATH) # This could use ScraperAPI or direct based on USE_SCRAPER_API

        if initial_soup:
            visited_paths.add(BASE_URL + START_PATH) # Mark initial page as visited
            top_level_nodes = get_child_nodes(initial_soup, BASE_URL + START_PATH)
            
            print(f"Found {len(top_level_nodes)} top-level categories.")
            for i, node in enumerate(top_level_nodes):
                node_path = node['path'] # Path is relative like /amazon.co.uk/browseNodeLookup/XYZ.html
                if not node_path: # Should not happen for top-level categories if they are not leaves
                    if node.get("is_leaf"):
                         print(f"  INFO: Top-level item '{node['name']}' is a leaf. Writing to CSV.")
                         if csv_writer_global:
                             csv_writer_global.writerow({"id": node['id'], "name": node['name']})
                             if csv_file_object_global: csv_file_object_global.flush()
                    else:
                        print(f"  WARN: Top-level node '{node['name']}' has no path and is not leaf. Skipping.")
                    continue

                print(f"\nProcessing top-level category {i+1}/{len(top_level_nodes)}: '{node['name']}' ({node_path})" )
                
                if node_path in completed_top_level_categories:
                    print(f"  INFO: Category '{node['name']}' ({node_path}) already marked as completed. Skipping.")
                    continue
                
                if node.get("is_leaf"): # Should ideally not be true for a top-level category with a path
                    print(f"  INFO: Top-level category '{node['name']}' marked as leaf in table. Writing to CSV.")
                    if csv_writer_global:
                         csv_writer_global.writerow({"id": node['id'], "name": node['name']})
                         if csv_file_object_global: csv_file_object_global.flush()
                    # Also mark as completed since it's a leaf at top-level
                    completed_top_level_categories.add(node_path)
                    append_line_to_file(node_path, COMPLETED_TOP_LEVEL_FILE)
                    save_set_to_json(visited_paths, VISITED_PATHS_FILE) # Save progress
                elif node_path:
                    scrape_leaf_nodes(node_path, visited_paths)
                    # After successfully scraping this top-level node and all its children:
                    print(f"  SUCCESS: Finished processing top-level category: '{node['name']}' ({node_path})")
                    completed_top_level_categories.add(node_path)
                    append_line_to_file(node_path, COMPLETED_TOP_LEVEL_FILE)
                    # Save visited_paths frequently, after each top-level category
                    save_set_to_json(visited_paths, VISITED_PATHS_FILE) 
                
                time.sleep(0.5) 
        else:
            print(f"FATAL: Failed to fetch the initial page: {BASE_URL + START_PATH}. Cannot continue.")

    except KeyboardInterrupt:
        print("\nINFO: Keyboard interrupt detected. Shutting down gracefully...")
    except Exception as e:
        print(f"\nCRITICAL ERROR in main execution: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("\nINFO: Finalizing script operations.")
        if csv_file_object_global:
            print("INFO: Closing CSV file.")
            csv_file_object_global.close()
        print("INFO: Saving final visited paths state...")
        save_set_to_json(visited_paths, VISITED_PATHS_FILE)
        print("Scraping session ended.") 