import os
import json
import yaml
import requests
import pandas as pd
from datetime import datetime
from airflow import DAG
from psycopg2.extras import execute_values
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.models.variable import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from requests.exceptions import RequestException
from time import sleep
from typing import Tuple, Optional, Dict, List


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 9),
    'retries': 3,
}

dag = DAG(
    'import_data',
    default_args=default_args,
    description='Import/store raw data from API endpoints',
    schedule_interval=None,
    catchup=False
)

def load_endpoints_config() -> Dict:
    config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config', 'endpoints.yml')
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)['resources']

#====== Get access token ======#
@task
def get_token() -> Tuple[Optional[str], Optional[str]]:
    max_retries = 3
    retry_delay = 5
    hook = BaseHook.get_connection("api_auth")
    url = Variable.get("api_token_url")
    payload = {
        "username": hook.login,
        "password": hook.password
    }
    
    for attempt in range(max_retries):
        try:
            print(f"Attempt {attempt + 1} of {max_retries}")
            response = requests.post(url, data=payload)
            response.raise_for_status()
            
            access_token = response.json().get("access_token")
            refresh_token = response.json().get("refresh_token")
            if not access_token:
                print("Error: Access token not found in response")
                return None, None
                
            print(f"Token retrieved successfully on attempt {attempt + 1}")
            return access_token, refresh_token
            
        except RequestException as e:
            if attempt == max_retries-1:
                print(f"Attempt {attempt+1} failed. Retrying in {retry_delay} seconds...")
                sleep(retry_delay)
                return None, None
            
        except (ValueError, json.JSONDecodeError) as e:
            print(f"Error: Invalid response format: {str(e)}")
            return None, None
            
    print("Error: Failed to retrieve token after all retry attempts")
    return None, None

@task
def refresh_token(refresh_token: str) -> Optional[str]:
    refresh_url = Variable.get("api_refresh_token_url")
    
    try:
        print("Attempting to refresh token...")
        refresh_response = requests.post(refresh_url, headers={"Authorization": f"Bearer {refresh_token}"})
        refresh_response.raise_for_status()
        
        new_access_token = refresh_response.json().get("access_token")
        
        if not new_access_token:
            print("Error: Refresh token not found in response")
            return None
            
        print("Token refreshed successfully")
        return new_access_token
        
    except (RequestException, ValueError, json.JSONDecodeError) as e:
        print(f"Error: Failed to refresh token: {str(e)}")
        return None

#====== Fetch data from API endpoints ======#
@task
def fetch_and_store_data(resource_name: str, token_info: Tuple[Optional[str], Optional[str]]) -> Optional[str]:
    all_data = []
    
    config = load_endpoints_config()[resource_name]
    limit = config['limit']
    skip = 0

    try:
        token, refresh_token_str = token_info
        if not token:
            if refresh_token_str:
                token = refresh_token(refresh_token_str).output
            if not token:
                return None

        headers = {"Authorization": f"Bearer {token}"}
        url = config['endpoint']
        
        while True:
            params = {
                "skip": skip,
                "limit": limit
            }
            response = requests.get(url, headers=headers, params=params)
            if response.status_code == 500:
                print("Received 500 error, retrying ...")
                response = requests.get(url, headers=headers, params=params)
                if response.status_code == 500:
                    break
            response.raise_for_status()
            data = response.json()
            if not data:
                break
            all_data.extend(data)
            skip += limit
            sleep(0.5)
            
        now = datetime.now()
        date_path = now.strftime("%Y-%m-%d")
        timestamp = now.strftime("%Y%m%d_%H%M%S")
        
        output_dir = config['local_path'].format(date_path=date_path)
        os.makedirs(output_dir, exist_ok=True)
        
        filename = config['file_name'].format(timestamp=timestamp)
        file_path = os.path.join(output_dir, filename)
        
        with open(file_path, "w") as f:
            json.dump(all_data, f, indent=4)
        
        print(f"Successfully fetched and stored {len(all_data)} records for {resource_name}")
        return file_path
    
    except Exception as e:
        print(f"Error: Failed to fetch and store {resource_name}: {str(e)}")
        return None

#====== Load JSON files into database ======#
@task
def load_json_to_database(file_paths: Dict[str, str]) -> None:
    postgres_hook = PostgresHook(postgres_conn_id="postgresdb")
    
    table_schemas = {
        'carts': """
            CREATE TABLE IF NOT EXISTS raw_carts (
                id SERIAL PRIMARY KEY,
                sale_date TIMESTAMP,
                total_amount NUMERIC,
                shipping_info JSONB,
                created_at TIMESTAMP,
                status VARCHAR(50),
                customer_id INTEGER,
                logistic_id INTEGER,
                items JSONB,
                payment_info JSONB
            )
        """,
        'customer': """
            CREATE TABLE IF NOT EXISTS raw_customer (
                id SERIAL PRIMARY KEY,
                phone VARCHAR(50),
                city VARCHAR(100),
                address VARCHAR(255),
                full_name VARCHAR(255),
                email VARCHAR(255),
                created_at TIMESTAMP
            )
        """,
        'logistict': """
            CREATE TABLE IF NOT EXISTS raw_logistict (
                id SERIAL PRIMARY KEY,
                company_name VARCHAR(255),
                service_type VARCHAR(50),
                origin_warehouse VARCHAR(255),
                contact_phone VARCHAR(50),
                created_at TIMESTAMP
            )
        """,
        'products': """
            CREATE TABLE IF NOT EXISTS raw_products (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255),
                category VARCHAR(255),
                created_at TIMESTAMP,
                price NUMERIC
            )
        """
    }
    
    def process_json_file(file_path: str) -> List[Dict]:
        with open(file_path, 'r') as f:
            data = json.load(f)
            return data if isinstance(data, list) else [data]
    
    with postgres_hook.get_conn() as conn:
        with conn.cursor() as cur:
            
            for resource_name in table_schemas.keys():
                table_name = f"raw_{resource_name}"
                cur.execute(f"DROP TABLE IF EXISTS {table_name}")
            conn.commit()
            
            for table_name, schema in table_schemas.items():
                cur.execute(schema)
            conn.commit()
            
            for resource_name, file_path in file_paths.items():
                if not file_path:
                    print(f"Skipping {resource_name} due to missing file path")
                    continue
                
                table_name = f"raw_{resource_name}"
                print(f"Loading data into {table_name} from {file_path}")
                
                try:
                    data = process_json_file(file_path)
                    if not data:
                        print(f"No data found in {file_path}")
                        continue
                    
                    df = pd.DataFrame(data)
                    
                    timestamp_cols = ['created_at', 'sale_date']
                    for col in timestamp_cols:
                        if col in df.columns:
                            df[col] = pd.to_datetime(df[col])
                    
                    if resource_name == 'carts':
                        json_cols = ['shipping_info', 'items', 'payment_info']
                        for col in json_cols:
                            if col in df.columns:
                                df[col] = df[col].apply(json.dumps)
                    
                    columns = list(df.columns)
                    values = [tuple(x) for x in df.to_numpy()]
                    
                    insert_query = f"""
                        INSERT INTO {table_name} ({', '.join(columns)})
                        VALUES %s
                    """
                    execute_values(cur, insert_query, values)
                    
                    conn.commit()
                    print(f"Successfully loaded {len(data)} records into {table_name}")
                    
                except Exception as e:
                    conn.rollback()
                    print(f"Error loading data into {table_name}: {str(e)}")
                    raise

with dag:
    token_info = get_token()
    
    resources = load_endpoints_config()
    fetch_tasks = {}
    
    for resource_name in resources.keys():
        fetch_tasks[resource_name] = fetch_and_store_data(resource_name, token_info)
    
    load_json_to_database(fetch_tasks)