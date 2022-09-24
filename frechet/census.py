import requests
import json

import pandas as pd

from frechet.geom import GEOM, PARENT

from typing import *

# NOTE: use this for enums, keep census queries tied to the fips geography constructs

CENSUS_DS = Literal["dec"]
CENSUS_DS_MAP: Dict[str, List[str]] = {  # TODO availability years in here too?
    "dec": [  # https://www.census.gov/data/developers/data-sets/decennial-census.html
        "pl",  # https://api.census.gov/data/2020/dec/pl.html
        "pes",  # https://api.census.gov/data/2020/dec/pes.html
    ]
}
GEOM_NAME_MAP: Dict[GEOM, str] = {
    "tracts": "tract",
    "block_groups": "block group",
    "county_sub": "county subdivision",
    "blocks": "block"
}
GEOM_ID_MAP: Dict[GEOM, str] = {
    "tracts": "140",
    "block_groups": "150",
    "county_sub": "060",
    "blocks": "100"
}


def construct_request_url(
    ds: CENSUS_DS,
    sub_ds: str,
    geom: GEOM,
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


def validate_geom(ds: CENSUS_DS, sub_ds: str, year: int, parent: PARENT, geom: GEOM) -> pd.DataFrame:
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
        
        