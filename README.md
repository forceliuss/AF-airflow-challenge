# Airflow Variables and DAGs

## Airflow Variables

The following Airflow variables are used in the pipeline:
(The upload file is in the `config` directory)

- `api_url`: The URL of the API.
- `api_token_url`: The URL of the API token.
- `api_refresh_token_url`: The URL of the API refresh token.
- `api_products_url`: The URL of the API products.
- `api_carts_url`: The URL of the API carts.
- `api_customer_url`: The URL of the API customer.
- `api_logistict_url`: The URL of the API logistict.

## DAGs Description

### Carts DAG

This DAG is responsible for extracting, transforming, and loading cart data from the API.

- **Tasks**:
  - Extract cart data from the API.
  - Transform the data to the required format.
  - Explode the items in the cart to get a list of products.
  - Load the transformed data into the raw, staging, and marts layers.

### Customers DAG

This DAG is responsible for extracting, transforming, and loading customer data from the API.

- **Tasks**:
  - Extract customer data from the API.
  - Transform the data to the required format.
  - Load the transformed data into the raw, staging, and marts layers.

### Logistics DAG

This DAG is responsible for extracting, transforming, and loading logistics data from the API.

- **Tasks**:
  - Extract logistics data from the API.
  - Transform the data to the required format.
  - Load the transformed data into the raw, staging, and marts layers.

### Products DAG

This DAG is responsible for extracting, transforming, and loading product data from the API.

- **Tasks**:
  - Extract product data from the API.
  - Transform the data to the required format.
  - Load the transformed data into the raw, staging, and marts layers.

## How to Run

To run the project, follow these steps:

1. Clone the repository:

   ```sh
   git clone <repository_url>
   cd <repository_directory>
   ```

2. Initialize the Airflow database:

   ```sh
   docker compose up airflow-init
   ```

3. Start the Airflow services:

   ```sh
   docker compose up
   ```

(For testing purposes) 4. Access the Airflow web interface at `http://localhost:8080` and trigger the DAGs.

## Data Separation: Raw, Staging, Intermediate, and Marts

### Raw Data

Raw data is the initial data extracted from the source systems (API in this case). It is stored in its original format without any transformations. This layer serves as a backup and allows for reprocessing if needed. The raw data is stored in the `local_storage/raw` directory.

### Staging Data (Dbt)

Staging data is the intermediate layer where data undergoes initial transformations and cleaning. This layer is used to prepare the data for further processing and loading into the marts layer. It helps in identifying and handling any data quality issues.

##### Staging Data Files

- `stg_carts.sql` - Cart data with carts status and total items.
- `stg_customers.sql` - Customer data.
- `stg_logistics.sql` - Logistics data.
- `stg_products.sql` - Product data.

### Intermediate Data (Dbt)

Intermediate data is the layer where data undergoes further transformations and cleaning. This layer is used to prepare the data for further processing and loading into the marts layer. It helps in identifying and handling any data quality issues.

##### Intermediate Data Files

- `int_cart_items.sql` - Cart items data with exploded items.

### Marts Data (Dbt)

Marts data is the final layer where data is fully processed, cleaned, and transformed. This layer is used for reporting, analysis, and other business purposes. The data in this layer is considered reliable and ready for consumption by end-users and applications.

##### Marts Data Files

- `finance/order_financials.sql` - Order financials data, finance related metrics.
- `marketing/customer_acquisition.sql` - Customer acquisition data, marketing related metrics.
- `operations/order_performance.sql` - Order performance data, operations related metrics.
