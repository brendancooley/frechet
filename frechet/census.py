import requests

import pandas as pd

from typing import *

# NOTE: use this for enums, keep census queries tied to the fips geography constructs

CENSUS_DS = Literal["dec"]

CENSUS_DS_MAP: Dict[str, List[str]] = { # TODO availability years in here too?
    "dec": [  # https://www.census.gov/data/developers/data-sets/decennial-census.html
        "pl",  # https://api.census.gov/data/2020/dec/pl.html
        "pes",  # https://api.census.gov/data/2020/dec/pes.html
    ]
}

def _validate_ds(ds: CENSUS_DS, sub_ds: str) -> bool:
    if ds not in CENSUS_DS_MAP.keys():
        raise ValueError(f"Unknown dataset: {ds}")
    if sub_ds not in CENSUS_DS_MAP[ds]:
        raise ValueError(f"Unknown sub-dataset for {ds}: {sub_ds}")
    else:
        return True

def _validate_vars(ds: CENSUS_DS, sub_ds: str, year: int, vars: List[str]) -> pd.DataFrame:
    if _validate_ds():
        reqst = requests.get("https://api.census.gov/data/{year}/{ds}/{sub_ds}/variables.json")
        if reqst.status_code == 404:
            raise LookupError(f"No variables found for dataset {ds}, sub dataset {sub_ds} in year {year}. Check that year is valid at https://api.census.gov/data.html.")
        df = pd.DataFrame.from_dict(reqst.content['variables'], orient='index')[["label", "concept"]].sort_index()
        df = df.loc[~df.index.isin(["for", "in", "ucgid"])]
        df = df.loc[df['concept'].notna()]
        df = df.loc[df["label"] != "Geography"]
        valid_vars = df.index.tolist()
        if any(x not in valid_vars for x in vars):
            # TODO collect invalids and send back
            raise ValueError()
        else:
            return df
