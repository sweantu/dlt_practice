import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator
import duckdb

table_name = "homework"


# your code is here
@dlt.resource(name=table_name, write_disposition="replace")
def ny_taxi():
    client = RESTClient(
        base_url="https://us-central1-dlthub-analytics.cloudfunctions.net",
        paginator=PageNumberPaginator(base_page=1, total_path=None),
    )

    for page in client.paginate("data_engineering_zoomcamp_api"):
        yield page


pipeline_name = "ny_taxi_homework_pipeline"
dataset_name = "ny_taxi_homework_data"

pipeline = dlt.pipeline(
    pipeline_name=pipeline_name,
    destination="duckdb",
    dataset_name=dataset_name,
)

# load_info = pipeline.run(ny_taxi)
# print(load_info)


# A database '<pipeline_name>.duckdb' was created in working directory so just connect to it

# Connect to the DuckDB database
# conn = duckdb.connect(f"{pipeline_name}.duckdb")

# Set search path to the dataset
# conn.sql(f"SET search_path = '{dataset_name}'")

# Describe the dataset
# print(conn.sql("DESCRIBE").df())

# df = pipeline.dataset(dataset_type="default")[table_name].df()
# print(df)


with pipeline.sql_client() as client:
    res = client.execute_sql(
        f"""
            SELECT
            AVG(date_diff('minute', trip_pickup_date_time, trip_dropoff_date_time))
            FROM {table_name};
            """
    )
    # Prints column values of the first row
    print(res)
