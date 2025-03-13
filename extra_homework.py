import dlt
import pandas as pd
import requests
from bs4 import BeautifulSoup
from io import BytesIO

url_parquet = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"


def get_parquet_urls():
    """Get the urls from the TLC NYC webpage data"""
    response = requests.get(url_parquet)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    links = soup.find_all("a", href=True)

    parquet_urls = [link["href"] for link in links if link["href"].endswith(".parquet")]

    return parquet_urls


# get the urls and insert into a variable urls_list
urls_list = get_parquet_urls()

# to see the url
# print(urls_list[0])

parquet_urls = [
    f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-0{str(month)}.parquet"
    for month in range(1, 3)
]
print(parquet_urls)


extra_homework = "_extra_homework"


@dlt.resource(name=f"ny_taxi_data{extra_homework}", write_disposition="replace")
def ny_taxi():

    for url in parquet_urls:
        response = requests.get(url)
        response.raise_for_status()

        df = pd.read_parquet(BytesIO(response.content))
        yield df.to_dict(orient="records")


pipeline = dlt.pipeline(
    pipeline_name=f"taxi_data{extra_homework}",
    destination="bigquery",
    dataset_name=f"taxi_ny_data_yellow{extra_homework}",
    dev_mode=True,
)

info = pipeline.run(ny_taxi)
print(info)
