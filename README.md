# Airflow Variables and DAGs

## Airflow Variables

The following Airflow variables are used in the DAGs:

- `api_base_url`: Base URL for the API endpoints.
- `api_token`: Token for authenticating API requests.
- `api_refresh_token`: Refresh token for obtaining a new API token.
- `raw_data_path`: Path to store raw data.
- `stage_data_path`: Path to store staged data.
- `trusted_data_path`: Path to store trusted data.

## DAGs Description

### Carts DAG

This DAG is responsible for extracting, transforming, and loading cart data from the API.

- **Tasks**:
  - Extract cart data from the API.
  - Transform the data to the required format.
  - Load the transformed data into the raw, stage, and trusted layers.

### Customers DAG

This DAG is responsible for extracting, transforming, and loading customer data from the API.

- **Tasks**:
  - Extract customer data from the API.
  - Transform the data to the required format.
  - Load the transformed data into the raw, stage, and trusted layers.

### Logistics DAG

This DAG is responsible for extracting, transforming, and loading logistics data from the API.

- **Tasks**:
  - Extract logistics data from the API.
  - Transform the data to the required format.
  - Load the transformed data into the raw, stage, and trusted layers.

### Products DAG

This DAG is responsible for extracting, transforming, and loading product data from the API.

- **Tasks**:
  - Extract product data from the API.
  - Transform the data to the required format.
  - Load the transformed data into the raw, stage, and trusted layers.

## How to Run

To run the project, follow these steps:

1. Clone the repository:

   ```sh
   git clone <repository_url>
   cd <repository_directory>
   ```

2. Initialize the Airflow database:

   ```sh
   docker-compose up airflow-init
   ```

3. Start the Airflow services:

   ```sh
   docker-compose up
   ```

4. Access the Airflow web interface at `http://localhost:8080` and trigger the DAGs.

## Data Separation: Raw, Stage, and Trusted

### Raw Data

Raw data is the initial data extracted from the source systems (API in this case). It is stored in its original format without any transformations. This layer serves as a backup and allows for reprocessing if needed.

### Stage Data

Stage data is the intermediate layer where data undergoes initial transformations and cleaning. This layer is used to prepare the data for further processing and loading into the trusted layer. It helps in identifying and handling any data quality issues.

### Trusted Data

Trusted data is the final layer where data is fully processed, cleaned, and transformed. This layer is used for reporting, analysis, and other business purposes. The data in this layer is considered reliable and ready for consumption by end-users and applications.
