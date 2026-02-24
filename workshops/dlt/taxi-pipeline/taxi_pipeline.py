"""NYC taxi data pipeline using the Data Engineering Zoomcamp REST API."""

import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator


# API returns a root-level JSON array; pagination stops when a page is empty.
BASE_URL = "https://us-central1-dlthub-analytics.cloudfunctions.net"
PAGE_SIZE = 1000


@dlt.source
def nyc_taxi_rest_api_source() -> dlt.sources.DltSource:
    """Define dlt resources from the NYC taxi REST API (Data Engineering Zoomcamp)."""
    client = RESTClient(
        base_url=BASE_URL,
        paginator=PageNumberPaginator(
            base_page=1,
            total_path=None
        ),
        data_selector="$",  # Extract data from root-level array
    )

    @dlt.resource(name="taxi_trips")
    def get_taxi_trips():
        for page in client.paginate("data_engineering_zoomcamp_api"):
            yield page

    yield get_taxi_trips


taxi_pipeline = dlt.pipeline(
    pipeline_name="taxi_pipeline",
    destination="duckdb",
    dataset_name="nyc_taxi_data",
    progress="log",
)


if __name__ == "__main__":
    load_info = taxi_pipeline.run(nyc_taxi_rest_api_source())
    print(load_info)
