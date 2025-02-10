import os
import json
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.models.variable import Variable
from airflow.utils.dates import days_ago
from requests.exceptions import RequestException
from time import sleep
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 3,
}

dag = DAG(
    'fetch_customer',
    default_args=default_args,
    description='Fetch customer data from the API',
    schedule_interval=None,
    catchup=False
)

def _get_token(max_retries=3, retry_delay=5):
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
                return None
                
            print(f"Token retrieved successfully on attempt {attempt + 1}")
            return access_token, refresh_token
            
        except RequestException as e:
            if attempt == max_retries-1:
                print(f"Attempt {attempt+1} failed. Retrying in {retry_delay} seconds...")
                sleep(retry_delay)
                return None
            
        except (ValueError, json.JSONDecodeError) as e:
            print(f"Error: Invalid response format: {str(e)}")
            return None
            
    print("Error: Failed to retrieve token after all retry attempts")
    return None

def _refresh_token(refresh_token):
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
        
    except RequestException as e:
        print(f"Error: Failed to refresh token: {str(e)}")
        
    except (ValueError, json.JSONDecodeError) as e:
        print(f"Error: Invalid refresh response format: {str(e)}")
        
    print("Error: Failed to refresh token")
    return None

def fetch_and_store(file_type):

    all_data = []
    skip = 0
    limit = 50 

    try:
        print("Getting token...")
        token, refresh_token = _get_token()
        if not token:
            token = _refresh_token(refresh_token)

        headers = {"Authorization": f"Bearer {token}"}
        url = Variable.get("api_customer_url")
        
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
        
        output_dir = Variable.get(f"raw_{file_type}_path").format(date_path=date_path)
        os.makedirs(output_dir, exist_ok=True)
        
        filename = Variable.get(f"raw_{file_type}_file").format(timestamp=timestamp)
        file_path = os.path.join(output_dir, filename)
        
        with open(file_path, "w") as f:
            json.dump(all_data, f, indent=4)
        
        print(f"Successfully fetched and stored {len(all_data)} records")
        return file_path
    
    except Exception as e:
        print(f"Error: Failed to fetch and store {file_type}: {str(e)}")
        return None


fetch_customer_task = PythonOperator(
    task_id='fetch_customer',
    python_callable=fetch_and_store,
    op_args=["customer"],
    dag=dag
)

fetch_customer_task
