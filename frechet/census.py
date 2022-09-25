from multiprocessing.sharedctypes import Value
import requests
import json
from functools import cached_property, lru_cache

import pandas as pd

from frechet.url import CENSUS_API_BASE
from frechet.geom import GEOGRAPHY, PARENT

from typing import *

# NOTE: use this for enums, keep census queries tied to the fips geography constructs

CENSUS_DS = Literal["dec"]
CENSUS_DS_MAP: Dict[str, List[str]] = {  # TODO deprecate in favor of automatic url generation on Dataset class
    "dec": [  # https://www.census.gov/data/developers/data-sets/decennial-census.html
        "pl",  # https://api.census.gov/data/2020/dec/pl.html
        "pes",  # https://api.census.gov/data/2020/dec/pes.html
    ]
}
GEOM_NAME_MAP: Dict[GEOGRAPHY, str] = {
    "tracts": "tract",
    "block_groups": "block group",
    "county_sub": "county subdivision",
    "blocks": "block"
}
GEOM_ID_MAP: Dict[GEOGRAPHY, str] = {
    "tracts": "140",
    "block_groups": "150",
    "county_sub": "060",
    "blocks": "100"
}

@lru_cache
def available_datasets() -> pd.DataFrame:
    rsp = requests.get(f"{CENSUS_API_BASE}data.json")
    df = pd.DataFrame.from_dict(json.loads(rsp.content)['dataset'])
    df["name"] = df['c_dataset'].apply(lambda x: '/'.join(x))
    df = df.loc[df["c_vintage"].notna()].copy()
    df['year'] = df['c_vintage'].astype(int)
    return df

class Dataset:

    def __init__(self, name: str):
        self.name = self._validate_name(name=name)

        # available geographies
        # url constructor
    
    @cached_property
    def available_years(self) -> List[int]:
        df = available_datasets()
        return df.loc[df["name"] == self.name]['year'].unique().tolist()
    
    def available_geographies(self, year: int, with_st: bool = False, with_co: bool = False):
        self._validate_year()
        rsp = requests.get(
            f"{CENSUS_API_BASE}data/{year}/{self.name}/geography.json"
        )
        df = pd.DataFrame.from_dict(json.loads(rsp.content)["fips"])
        df = df.loc[df['name'].isin(GEOM_NAME_MAP.values())]
        # TODO how to handle wildcards?

    def variables(self, year: int) -> pd.DataFrame:
        self._validate_year()
        rsp = requests.get(
            f"{CENSUS_API_BASE}data/{year}/{self.name}/variables.json"
        )
        df = pd.DataFrame.from_dict(json.loads(rsp.content)["variables"], orient="index")[
            ["label", "concept"]
        ].sort_index()
        df = df.loc[~df.index.isin(["for", "in", "ucgid"])]
        df = df.loc[df["concept"].notna()]
        df = df.loc[df["label"] != "Geography"]
        return df
    
    def _validate_name(self, name: str) -> str:
        if not name in available_datasets()['name'].unique().tolist():
            raise ValueError(f"Dataset {name} not recognized. See {CENSUS_API_BASE}data.html for available datasets.")
        return name
    
    def _validate_year(self, year: int):
        if year not in self.available_years:
            raise ValueError(f"Year {year} is not valid for {self.name}. Available years are {' ,'.join(self.available_years)}")

    



def construct_request_url(
    ds: CENSUS_DS,
    sub_ds: str,
    geom: GEOGRAPHY,
    year: int,
    vars: List[str],
    st_fips: str,
    census_api_key: str,
    co_fips: Optional[str] = None,
):
    address_base = f"https://api.census.gov/data/{year}/{ds}/{sub_ds}?"
    vars_str = f"NAME,{','.join(vars)}"
    if co_fips is None:
        st_co_str = f"in=state:{st_fips}"
    else:
        st_co_str = f"in=state:{st_fips}&in=county:{co_fips}"
    key_str = f"key={census_api_key}"
    return f"{address_base}get={vars_str}&for={GEOM_NAME_MAP[geom]}:*&{st_co_str}&{key_str}"

def validate_ds(ds: CENSUS_DS, sub_ds: str) -> bool:
    if ds not in CENSUS_DS_MAP.keys():
        raise ValueError(f"Unknown dataset: {ds}")
    if sub_ds not in CENSUS_DS_MAP[ds]:
        raise ValueError(f"Unknown sub-dataset for {ds}: {sub_ds}")
    else:
        return True


def validate_vars(
    ds: CENSUS_DS, sub_ds: str, year: int, vars: List[str]
) -> pd.DataFrame:
    if validate_ds(ds=ds, sub_ds=sub_ds):
        reqst = requests.get(
            f"https://api.census.gov/data/{year}/{ds}/{sub_ds}/variables.json"
        )
        if reqst.status_code == 404:
            raise LookupError(
                f"No variables found for dataset {ds}, sub dataset {sub_ds} in year {year}. Check that year is valid at https://api.census.gov/data.html."
            )
        df = pd.DataFrame.from_dict(json.loads(reqst.content)["variables"], orient="index")[
            ["label", "concept"]
        ].sort_index()
        df = df.loc[~df.index.isin(["for", "in", "ucgid"])]
        df = df.loc[df["concept"].notna()]
        df = df.loc[df["label"] != "Geography"]
        valid_opts = df.index.tolist()
        invalid_vars = [x for x in vars if x not in valid_opts]
        if len(invalid_vars) > 0:
            raise ValueError(
                f"The following vars were not found in valid variable list for {ds}-{sub_ds}-{year}: {' ,'.join(invalid_vars)}"
            )
        else:
            return df


def validate_geom(ds: CENSUS_DS, sub_ds: str, year: int, parent: PARENT, geom: GEOGRAPHY) -> pd.DataFrame:
    if validate_ds(ds=ds, sub_ds=sub_ds):
        reqst = requests.get(
            f"https://api.census.gov/data/{year}/{ds}/{sub_ds}/geography.json"
        )
        if reqst.status_code == 404:
            raise LookupError(
                f"No variables found for dataset {ds}, sub dataset {sub_ds} in year {year}. Check that year is valid at https://api.census.gov/data.html."
            )
        df = pd.DataFrame.from_dict(json.loads(reqst.content)["fips"])
        if GEOM_ID_MAP[geom] not in df["geoLevelDisplay"].tolist():  # check that geom is supported globally
            raise ValueError(f"Geom {geom} not supported for {ds}-{sub_ds}-{year}")
        df_geom = df.loc[df["geoLevelDisplay"] == GEOM_ID_MAP[geom]].squeeze()
        if df_geom.isna()['optionalWithWCFor']:
            req = df_geom['requires']
        else:
            exempt = df_geom['optionalWithWCFor']
            req = [x for x in df_geom['requires'] if x not in exempt]
        provided = ['state', 'county'] if parent == 'county' else ['state']
        if not all(x in provided for x in req):
            raise ValueError(f"Geom {geom} not supported at parent level {parent} for {ds}-{sub_ds}-{year}. Required levels: {' ,'.join(req)}")
        return df
        
        