import os
import re
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Any, Literal

from dotenv import load_dotenv
import pandas as pd
import requests
from fastapi import FastAPI

from ucgid import UCGID

load_dotenv()
CENSUS_API_KEY = os.getenv("CENSUS_API_KEY")
app = FastAPI()


def fetch_variable_labels(year: str, dataset: str, table: str) -> dict[Any, Any]:
    url = f"https://api.census.gov/data/{year}/{dataset}/variables.json"
    response = requests.get(url, timeout=20)
    response.raise_for_status()

    variables = response.json()["variables"]
    return {
        code: info["label"]
        for code, info in variables.items()
        if code.startswith(table)
    }


def fetch_table_data(
    year: str, dataset: str, table: str, ucgid: str, api_key: str
) -> list:
    url = f"https://api.census.gov/data/{year}/{dataset}"
    params = {"get": f"group({table})", "ucgid": f"{ucgid}", "key": api_key}

    response = requests.get(url, params=params, timeout=20)
    response.raise_for_status()
    return response.json()


def build_dataframe(raw_data: list, label_map: dict) -> pd.DataFrame:
    if len(raw_data) < 2:
        raise ValueError("API response contains no data rows.")

    header, values = raw_data[0], raw_data[1]
    total_population = int(values[0])
    rows = []
    for code, value in zip(header, values):
        if code == "NAME" or not code.endswith("E"):
            continue

        label = (
            label_map.get(code, "")
            .replace(":", "")
            .replace("Estimate!!", "")
            .replace("Total!!", "")
        )

        if re.match(r"^.+!!.+$", label):
            continue

        # # Not completely accurate, but close enough for an MVP...
        # # Need to investigate further
        language = label.replace(",", "").split(" ")[0]
        if language == "Other" or language == "Speak" or language == "Total":
            continue

        population = int(value)
        percentage = round(population * 100 / total_population, 2)

        rows.append(
            {"language": language, "population": population, "percentage": percentage}
        )

    return pd.DataFrame(rows)


@app.get("/languages/us")
async def get_most_spoken_languages_by_ucgid(
    location_type: Literal["state", "county", "zcta"],
    state_fips: Optional[str] = None,
    county_fips: Optional[str] = None,
    zcta_code: Optional[str] = None,
):
    if location_type == "state":
        location_id = UCGID.from_state(state_fips=state_fips)
    elif location_type == "county":
        location_id = UCGID.from_county(state_fips=state_fips, county_fips=county_fips)
    elif location_type == "zcta":
        location_id = UCGID.from_zcta(zcta_code=zcta_code)
    else:
        raise ValueError("Unknown location type.")

    api_year = "2015" if location_id.instance == "zcta" else "2023"
    api_dataset = "acs/acs5" if location_id.instance == "zcta" else "acs/acs1"

    with ThreadPoolExecutor(max_workers=2) as executor:
        future_labels = executor.submit(
            fetch_variable_labels, api_year, api_dataset, "B16001"
        )
        future_data = executor.submit(
            fetch_table_data, api_year, api_dataset, "B16001", str(location_id), CENSUS_API_KEY
        )
        label_mapping = future_labels.result()
        raw_table_data = future_data.result()

    df = (
        build_dataframe(raw_table_data, label_mapping)
        .sort_values(by="population", ascending=False)
        .head(5)
    )

    return df.to_dict(orient="records")
