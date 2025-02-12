# Repository Structure

## Branching Strategy

- `main`: The main branch.
- `dev`: The development branch.
- `original-repo`: The original forked repository branch.

## Folder Structure

- `dags`: Contains the DAGs.
  - `dbt-project`: Contains the dbt project files.
- `config`: Contains the configuration files.
- `local_storage`: Contains the raw data (Json files).

## Open Questions

**_Improviments to be made in the code_**

- Optimize the data loading process.
- Schedule the DAGs to run daily or periodically.
- Add a process to handle the data quality checks.
- Improve the IDs data type and auto increment.
- Improve logging and error handling.

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

### Import Data DAG

This DAG is responsible for extracting, transforming, and loading data from the API.

- **Tasks**:
  - Extract raw data from the API and store it in the `local_storage/raw` directory.
  - Load the raw data into the dbt raw layer.

### Dbt Transform DAG

This DAG is responsible for transforming the data in the dbt raw layer.

- **Tasks**:
  - Extract raw data from the dbt raw layer.
  - Transform the data to the required table structure.
  - Load the transformed data into the dbt staging, intermediate and marts layers.
    **_More about this in the Data Separation section_**

# How to Run

To run the project, follow these steps:

1. Clone the repository:

   ```sh
   git clone <repository_url>
   cd <repository_directory>
   ```

2. Start the Airflow services:

   ```sh
   docker compose up -d --build
   ```

(For testing purposes) 3. Access the Airflow web interface at `http://localhost:8080`

(For testing purposes) 4. Trigger the DAGs (Import Data and Dbt Transform).

# Data Separation: Raw, Staging, Intermediate, and Marts

## Raw Data

Raw data is the initial data extracted from the source systems (API in this case). It is stored in its original format without any transformations. This layer serves as a backup and allows for reprocessing if needed. The raw data is stored in the `local_storage/raw` directory.

## Staging Data (Dbt)

Staging data is the intermediate layer where data undergoes initial transformations and cleaning. This layer is used to prepare the data for further processing and loading into the marts layer. It helps in identifying and handling any data quality issues.

### Staging Data Files

- `stg_carts.sql` - Cart data with carts status and total items.
- `stg_customers.sql` - Customer data.
- `stg_logistics.sql` - Logistics data.
- `stg_products.sql` - Product data.

## Intermediate Data (Dbt)

Intermediate data is the layer where data undergoes further transformations and cleaning. This layer is used to prepare the data for further processing and loading into the marts layer. It helps in identifying and handling any data quality issues.

### Intermediate Data Files

- `int_cart_items.sql` - Cart items data with exploded items.

## Marts Data (Dbt)

Marts data is the final layer where data is fully processed, cleaned, and transformed. This layer is used for reporting, analysis, and other business purposes. The data in this layer is considered reliable and ready for consumption by end-users and applications.

### Finance Division

- `finance/order_financials.sql` - Order financials data, finance related metrics.

| Metric Name         | Formula                                                  | SQL Name                 |
| ------------------- | -------------------------------------------------------- | ------------------------ |
| Product Cost        | QUANTITY _ (PRICE _ (1 - DISCOUNT))                      | TOTAL_COST_PRICE         |
| Total Selling Price | SUM(LINE_TOTAL)                                          | TOTAL_SELLING_PRICE      |
| Gross Profit        | SUM(LINE*TOTAL - (QUANTITY * (PRICE \_ (1 - DISCOUNT)))) | GROSS_PROFIT             |
| Net Profit          | GROSS_PROFIT                                             | NET_PROFIT               |
| Profit Margin       | (GROSS_PROFIT / ORDER_TOTAL) \* 100                      | PROFIT_MARGIN_PERCENTAGE |

**_ For simplicity, the discount applied to the product price as cost price. _**

### Marketing Division

- `marketing/customer_orders.sql` - Customer orders data, marketing related metrics.

| Metric Name            | Formula                                     | SQL Name               |
| ---------------------- | ------------------------------------------- | ---------------------- |
| Total Orders           | COUNT(DISTINCT CART_ID)                     | TOTAL_ORDERS           |
| Total Spent            | SUM(CART_TOTAL)                             | TOTAL_SPENT            |
| Avg Order Value        | AVG(CART_TOTAL)                             | AVG_ORDER_VALUE        |
| Unique Products Bought | COUNT(DISTINCT PRODUCT_ID)                  | UNIQUE_PRODUCTS_BOUGHT |
| Total Items Bought     | SUM(QUANTITY)                               | TOTAL_ITEMS_BOUGHT     |
| Avg Item Price         | TOTAL_SPENT / NULLIF(TOTAL_ITEMS_BOUGHT, 0) | AVG_ITEM_PRICE         |

### Operations Division

- `operations/order_performance.sql` - Order performance data, operations related metrics.

| Metric Name               | Formula                                        | SQL Name                  |
| ------------------------- | ---------------------------------------------- | ------------------------- |
| Unique Products Per Order | COUNT(DISTINCT PRODUCT_ID)                     | UNIQUE_PRODUCTS_PER_ORDER |
| Total Items Per Order     | SUM(QUANTITY)                                  | TOTAL_ITEMS_PER_ORDER     |
| Total Order Value         | SUM(LINE_TOTAL)                                | TOTAL_ORDER_VALUE         |
| Average Item Value        | ORDER_VALUE / NULLIF(TOTAL_ITEMS_PER_ORDER, 0) | AVG_ITEM_VALUE            |
