import os
import json
import requests
from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.models.variable import Variable
from requests.exceptions import RequestException
from time import sleep
from typing import Tuple, Optional


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
def fetch_and_store_data(data_type: str, token_info: Tuple[Optional[str], Optional[str]]) -> Optional[str]:
    all_data = []
    skip = 0
    limit = 50

    try:
        token, refresh_token_str = token_info
        if not token:
            if refresh_token_str:
                token = refresh_token(refresh_token_str).output
            if not token:
                return None

        headers = {"Authorization": f"Bearer {token}"}
        url = Variable.get(f"api_{data_type}_url")
        
        while True:
            params = {
                "skip": skip,
                "limit": limit
            }
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
        
        output_dir = Variable.get(f"raw_{data_type}_path").format(date_path=date_path)
        os.makedirs(output_dir, exist_ok=True)
        
        filename = Variable.get(f"raw_{data_type}_file").format(timestamp=timestamp)
        file_path = os.path.join(output_dir, filename)
        
        with open(file_path, "w") as f:
            json.dump(all_data, f, indent=4)
        
        print(f"Successfully fetched and stored {len(all_data)} records")
        return file_path
    
    except Exception as e:
        print(f"Error: Failed to fetch and store {data_type}: {str(e)}")
        return None

with dag:

    token_info = get_token()
    
    products_file = fetch_and_store_data("products", token_info)
    customer_file = fetch_and_store_data("customer", token_info)
    carts_file = fetch_and_store_data("carts", token_info)
    logistics_file = fetch_and_store_data("logistict", token_info) 