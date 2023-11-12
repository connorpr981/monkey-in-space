import urllib.request
import pandas as pd
import urllib.parse
import ssl
import logging
import json
from dotenv import load_dotenv
import os
import concurrent.futures
import time
import psycopg2
from sqlalchemy import create_engine
from sshtunnel import SSHTunnelForwarder
import boto3

# Initialize logging
logging.basicConfig(level=logging.INFO)

# Load environment variables
load_dotenv()

# Environment Variables
HTTP_COOKIE = os.environ.get("HTTP_COOKIE")
HTTP_USER_AGENT = os.environ.get("HTTP_USER_AGENT")

# Constants
MAX_PARCELS = 200
MAX_WORKERS = 8
BATCH_SLEEP_TIME = 2  # Time in seconds

if not HTTP_COOKIE or not HTTP_USER_AGENT:
    raise EnvironmentError("Required environment variables are missing.")

def create_db_connection(usr, pwd, local=False):
    if local:
        server = SSHTunnelForwarder(
            (os.environ['BH_IP'], 22),
            ssh_username='ec2-user', 
            ssh_private_key='real-estate-db-key.pem',
            remote_bind_address=(os.environ['DB_HOST'], 5432)
        )
        server.start()
        engine = create_engine(
            f"postgresql+psycopg2://{usr}:{pwd}@localhost:{server.local_bind_port}/{os.environ['DB_NAME']}"
        )
        return server, engine
    engine = create_engine(f"postgresql+psycopg2://{usr}:{pwd}@{os.environ['DB_HOST']}:5432/{os.environ['DB_NAME']}")
    return engine

def write_to_table(engine, df):
    df.to_sql('chesco', engine, if_exists='append', index=False)

def load_db_secret():
    session = boto3.session.Session()
    client = session.client('secretsmanager')
    secret_response = client.get_secret_value(
        SecretId=os.environ['DB_SECRET']
    )
    db_username = json.loads(secret_response['SecretString'])['username']
    db_password = json.loads(secret_response['SecretString'])['password']
    return db_username, db_password

def log_to_file(filename, message):
    """
    Append the given message to the specified file.

    Parameters:
    - filename (str): Name of the file to write to.
    - message (str): Message to append to the file.
    """
    with open(filename, 'a') as f:
        f.write(message + '\n')

def construct_query_with_range(street_name, st_num_min, st_num_max):
    """
    Construct a query string with specified parameters.
    
    Parameters:
    - street_name (str): The name of the street.
    - st_num_min (str): The minimum street number.
    - st_num_max (str): The maximum street number.
    
    Returns:
    - str: The constructed query string.
    """
    query_parts = [f"ST_NAME LIKE UPPER('{street_name}')"]
    
    if st_num_min and st_num_max:
        query_parts.append(f"(ST_NUM BETWEEN '{st_num_min}' AND '{st_num_max}')")
    
    return " AND ".join(query_parts)


def iterative_search(street, st_num_min=0, st_num_max=10000, all_parcels=None, log_filename="search_log.txt"):
    """
    Iteratively search parcels within a given street number range. This function uses an iterative 
    (stack-based) approach to divide the search range when the number of parcels is too large.
    
    When a single street number/street name combination results in the maximum number of parcels,
    the function only retrieves the top result and flags it in a new column called 'max_parcels'.
    
    Parameters:
    - street (str): The street name to search for.
    - st_num_min (int, optional): The minimum street number in the range. Default is 0.
    - st_num_max (int, optional): The maximum street number in the range. Default is 50000.
    - all_parcels (list, optional): A list to which parcels are appended during the search.
                                  This list will be used to update parcels in place. If None, 
                                  a new list is created. Default is None.
    - log_filename (str, optional): The name of the log file where search results will be recorded. Default is "search_log.txt".
    
    Returns:
    - list: The list of all parcels found within the specified range for the given street.
    """
    if all_parcels is None:
        all_parcels = []
    
    stack = [(st_num_min, st_num_max)]

    while stack:
        st_num_min, st_num_max = stack.pop()
        
        # Handle case where range is a single number
        if st_num_min == st_num_max:
            single_query = construct_query_with_range(street, str(st_num_min), str(st_num_max))
            parcels_single = fetch_parcels(single_query)
            log_to_file(log_filename, f"Queried single range: {st_num_min}. Found {len(parcels_single)} parcels.")
            
            if len(parcels_single) == MAX_PARCELS:
                # Take the first parcel and add a "max_parcels" flag
                parcel = parcels_single[0]
                parcel['max_parcels'] = True
                all_parcels.append(parcel)
                logging.warning(f"{st_num_min} {street} returns the maximum number of parcels. Top result has been added with 'max_parcels' flag.")
            else:
                all_parcels.extend(parcels_single)
            continue
        
        mid_point = (st_num_min + st_num_max) // 2

        query_lower_half = construct_query_with_range(street, str(st_num_min), str(mid_point))
        query_upper_half = construct_query_with_range(street, str(mid_point + 1), str(st_num_max))

        parcels_lower_half = fetch_parcels(query_lower_half)
        parcels_upper_half = fetch_parcels(query_upper_half)

        # Log the results
        log_to_file(log_filename, f"Queried range: {st_num_min} to {mid_point}. Found {len(parcels_lower_half)} parcels.")
        log_to_file(log_filename, f"Queried range: {mid_point + 1} to {st_num_max}. Found {len(parcels_upper_half)} parcels.")

        # Check if lower half needs to be split
        if len(parcels_lower_half) >= MAX_PARCELS:
            stack.append((st_num_min, mid_point))
        else:
            all_parcels.extend(parcels_lower_half)

        # Check if upper half needs to be split
        if len(parcels_upper_half) >= MAX_PARCELS:
            stack.append((mid_point + 1, st_num_max))
        else:
            all_parcels.extend(parcels_upper_half)

    return all_parcels

def filter_df_by_street(df, street, st_type=None, muni_id=None):
    """
    Filter the dataframe based on the street name, street type, and municipal ID.
    
    Parameters:
    - df (pandas.DataFrame): The DataFrame to filter.
    - street (str): The street name to filter by.
    - st_type (str, optional): The street type to filter by.
    - muni_id (str, optional): The municipal ID to filter by.
    
    Returns:
    - pandas.DataFrame: The filtered DataFrame.
    """
    conditions = (df['ST_NAME'] == street)
    if st_type:
        conditions &= (df['ST_TYPE'] == st_type)
    if muni_id:
        conditions &= (df['MUNI_ID'] == muni_id)
    return df[conditions]

def fetch_parcels(query: str) -> list:
    """
    Fetch parcels based on a given query from a specified endpoint.
    
    Parameters:
    - query (str): The query string to fetch parcels.
    
    Returns:
    - list: A list of parcels fetched from the endpoint.
    """
    headers = {
        "Cookie": HTTP_COOKIE,
        "Accept": "application/json, text/plain, */*",
        "Accept-Encoding": "gzip, deflate, br",
        "Host": "arcweb.chesco.org",
        "User-Agent": HTTP_USER_AGENT,
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://arcweb.chesco.org/CV4/",
        "Connection": "keep-alive",
    }
    
    url = f"https://arcweb.chesco.org/CV3Service/CV3Service1.svc/JsonService/GetParcelAttributes/{urllib.parse.quote(query)}/PIN_MAP"
    ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    ctx.options |= 0x4  # OP_LEGACY_SERVER_CONNECT
    req = urllib.request.Request(url, headers=headers)
    
    try:
        response = urllib.request.urlopen(req, context=ctx)
        if response.status == 200:
            return json.loads(response.read().decode()).get('PARCELS', [])
        else:
            logging.error(f"Failed to fetch data: {response.status}, {response.read().decode()}")
            return []
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return []

def identify_streets_with_max_parcels(df_parcels):
    """
    Identify streets which have the maximum number of parcels.
    
    Parameters:
    - df_parcels (pandas.DataFrame): The DataFrame containing parcel data.
    
    Returns:
    - list: A list of street names with the maximum number of parcels.
    """
    return df_parcels[df_parcels.groupby('ST_NAME')['ST_NAME'].transform('size') == MAX_PARCELS]['ST_NAME'].unique()

def recursive_fetch_for_max_streets(streets_with_max_parcels, all_parcels):
    """
    Recursively fetch parcels for streets which have the maximum number of parcels.
    
    Parameters:
    - streets_with_max_parcels (list): List of streets having the max parcels.
    - all_parcels (list): A list to which parcels are appended during the fetch.
    
    Returns:
    - None. The all_parcels list is updated in-place.
    """
    for street in streets_with_max_parcels:
        iterative_search(street, 0, 50000, all_parcels)  # Using default street number range.

def save_results_to_csv(all_parcels):
    """
    Save the list of parcels to a CSV file.
    
    Parameters:
    - all_parcels (list): The list of all parcels.
    
    Returns:
    - None. Results are saved to 'parcels_output.csv'.
    """
    df_parcels = pd.DataFrame(all_parcels)
    df_parcels.drop_duplicates(inplace=True)
    df_parcels.to_csv('parcels_output.csv', index=False)
    print("Saved results to parcels_output.csv")

def main():
    """
    The main execution function that manages the workflow of fetching parcels, 
    recursively fetching for streets with the maximum number of parcels, and saving results to a CSV.
    
    Returns:
    - None.
    """
    try:
        df = pd.read_csv('chesco_streets.csv')
        
        # Generate queries for fetching parcels
        queries = [construct_query_with_range(street, 0, 10000) for street in df['Street Name']]
        
        # Fetch parcels based on generated queries
        all_parcels = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Process the queries in batches
            for i in range(0, len(queries), MAX_WORKERS):
                batch_queries = queries[i:i+MAX_WORKERS]
                results = list(executor.map(fetch_parcels, batch_queries))
                all_parcels.extend([item for sublist in results for item in sublist])
                # Sleep between batches
                if i + MAX_WORKERS < len(queries):  # If there's another batch coming, sleep
                    time.sleep(BATCH_SLEEP_TIME)
        
        # Identify streets with max parcels
        streets_with_max_parcels = identify_streets_with_max_parcels(pd.DataFrame(all_parcels))
        
        # Recursively fetch parcels for streets with max parcels
        recursive_fetch_for_max_streets(streets_with_max_parcels, all_parcels)
        
        
        db_username, db_password = load_db_secret()
        engine = create_db_connection(db_username, db_password)
        # Upload results to SQL table
        write_to_table(engine, df)
    except Exception as e:
        logging.error(f"An error occurred: {e}")

if __name__ == '__main__':
    main()