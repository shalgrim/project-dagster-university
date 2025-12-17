import dagster as dg
import requests

from dagster_essentials.defs.assets import constants


@dg.asset
def taxi_trips_file() -> None:
    """
    raw parquet files for the taxi trips dataset.
    Sourced from the NYC Open Data portal.
    """
    month_to_fetch = "2023-03"
    raw_trips = requests.get(
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month_to_fetch}.parquet"
    )

    with open(
        constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch), "wb"
    ) as output_file:
        output_file.write(raw_trips.content)

@dg.asset
def taxi_zones_file() -> None:
    """
    csv file with unique identifier and name for each part of NYC as a distinct taxi zone
    Sourced from the NYC Open Data portal.
    """
    # TODO: set up my file watchers on this machine
    raw_zones = requests.get("https://community-engineering-artifacts.s3.us-west-2.amazonaws.com/dagster-university/data/taxi_zones.csv")

    with open(constants.TAXI_ZONES_FILE_PATH, "wb") as output_file:
        output_file.write(raw_zones.content)

