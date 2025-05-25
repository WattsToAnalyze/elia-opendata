"""
Demo script showing real usage of the Elia OpenData API client.
This script demonstrates fetching actual data from various datasets including
solar generation, wind generation, and total load data.
"""
import logging
import argparse
from elia_opendata.client import EliaClient
from elia_opendata.datasets import Dataset
from elia_opendata.error import APIError, ConnectionError, RateLimitError

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_client_initialization():
    """Test client initialization"""
    client = EliaClient()
    logger.info("Initialized Elia API client")
    return client

def pretty_print_catalog(catalog, md_path=None):
    """
    Print a table of all datasets in the catalog with columns: ID, Name, Label.
    Optionally write the table to a Markdown file.
    """
    from tabulate import tabulate
    headers = ["ID", "Theme", "Label"]
    rows = []
    for entry in catalog:
        dataset_id = getattr(entry, 'id', 'N/A')
        theme = ', '.join(getattr(entry, 'theme', []) or [])
        label = getattr(entry, 'title', 'N/A')
        rows.append([dataset_id, theme, label])
    table = tabulate(rows, headers, tablefmt="github")
    print(table)
    if md_path:
        with open(md_path, "w", encoding="utf-8") as f:
            f.write(f"# Elia OpenData Catalog\n\n")
            f.write(table)
            f.write("\n")

def fetch_all_catalog_entries(client, page_size=50):
    """
    Fetch all catalog entries from the API, handling pagination.
    Returns a list of all catalog entries.
    """
    all_entries = []
    offset = 0
    while True:
        batch = client.get_catalog(limit=page_size, offset=offset)
        if not batch:
            break
        all_entries.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size
    return all_entries

def test_catalog_fetch(client):
    """Test fetching the catalog and print first 10 entries as ID and label"""
    logger.info("Fetching all catalog information (all pages)...")
    catalog = fetch_all_catalog_entries(client)
    logger.info(f"Found {len(catalog)} datasets in catalog")
    # Print ID and label for first 10 entries
    for i, entry in enumerate(catalog[:10]):
        dataset_id = getattr(entry, 'id', 'N/A')
        label = getattr(entry, 'title', 'N/A')
        print(f"{i+1}. ID: {dataset_id} | Label: {label}")
    # Print full catalog as table and write to Markdown
    pretty_print_catalog(catalog, md_path="elia_catalog.md")
    return catalog

def test_solar_dataset(client):
    """Test getting solar dataset details"""
    logger.info("Fetching metadata for solar (photovoltaic) generation dataset...")
    solar_dataset = client.get_dataset(Dataset.PV_PRODUCTION)
    try:
        raw_data = solar_dataset._raw['dataset']
        metas = raw_data['metas']['default']
        logger.info("Solar dataset details:")
        logger.info(f"- Title: {metas['title']}")
        logger.info(f"- Description: {metas['description']}")
        logger.info(f"- Dataset ID: {raw_data['dataset_id']}")
        logger.info(f"- Last modified: {metas['modified']}")
        logger.info(f"- Available features: {', '.join(raw_data['features'])}")
        logger.info(f"- Fields: {', '.join(f['name'] for f in raw_data['fields'])}")
        logger.info(f"- Record count: {metas['records_count']}")
    except (AttributeError, KeyError) as e:
        logger.error(f"Failed to access dataset attributes: {e}")
        logger.debug("Dataset structure:", exc_info=True)
    return solar_dataset

def test_dataset_search(client):
    """Test searching datasets with keywords"""
    logger.info("\nSearching for datasets in catalog...")
    keywords = ['solar', 'pv', 'generation', 'load', 'consumption', 'power']
    found_datasets = []
    offset = 0
    limit = 10
    total_checked = 0

    while True:
        logger.info(f"Fetching datasets {offset+1} to {offset+limit}...")
        catalog = client.get_catalog(limit=limit, offset=offset)
        if not catalog:
            break
        total_checked += len(catalog)
        for entry in catalog:
            if hasattr(entry, 'dataset') and entry.dataset:
                metas = entry.dataset.get('metas', {}).get('default', {})
                title = metas.get('title', '').lower()
                desc = metas.get('description', '').lower()
                if any(keyword in title or keyword in desc for keyword in keywords):
                    dataset_id = entry.dataset.get('dataset_id')
                    theme = ', '.join(metas.get('theme', []))
                    logger.info(f"\nDataset: {dataset_id}")
                    logger.info(f"Title: {metas.get('title')}")
                    logger.info(f"Theme: {theme}")
                    # logger.info(f"Description: {metas.get('description')}")
                    # logger.info(f"Last modified: {metas.get('modified')}")
                    logger.info(f"Records count: {metas.get('records_count')}")
                    logger.info("-" * 80)
                    found_datasets.append((dataset_id, metas.get('title')))
        if len(catalog) < limit:
            break
        offset += limit

    if not found_datasets:
        logger.info(f"No matching datasets found after checking {total_checked} entries in the catalog.")

    return found_datasets

def run_test(test_name):
    """Run a specific test by name"""
    try:
        client = None
        catalog = None
        
        if test_name == "init":
            client = test_client_initialization()
        elif test_name == "catalog":
            client = test_client_initialization()
            catalog = test_catalog_fetch(client)
        elif test_name == "solar":
            client = test_client_initialization()
            test_solar_dataset(client)
        elif test_name == "search":
            client = test_client_initialization()
            test_dataset_search(client)
        else:
            logger.error(f"Unknown test: {test_name}")
            return

    except ConnectionError as e:
        logger.error(f"Connection to API failed: {str(e)}")
    except RateLimitError as e:
        logger.error(f"API rate limit exceeded: {str(e)}")
    except APIError as e:
        logger.error(f"API Error occurred: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error occurred: {str(e)}")
        logger.debug("Exception details:", exc_info=True)

def main():
    parser = argparse.ArgumentParser(description='Run Elia OpenData API tests')
    parser.add_argument('test', choices=['init', 'catalog', 'solar', 'search'],
                      help='Test to run: init (client initialization), catalog (fetch catalog), '
                           'solar (solar dataset), search (search datasets)')
    
    args = parser.parse_args()
    run_test(args.test)

if __name__ == "__main__":
    main()