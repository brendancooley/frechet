from multiprocessing.sharedctypes import Value
import requests
import json
from functools import cached_property, lru_cache

import pandas as pd

from frechet.url import CENSUS_API_BASE
from frechet.geom import GEOGRAPHY, PARENT
from frechet.settings import CENSUS_API_KEY

from typing import *

GEOM_NAME_MAP: Dict[GEOGRAPHY, str] = {
    "tracts": "tract",
    "block_groups": "block group",
    "county_sub": "county subdivision",
    "blocks": "block",
}


@lru_cache
def available_datasets() -> pd.DataFrame:
    rsp = requests.get(f"{CENSUS_API_BASE}data.json")
    df = pd.DataFrame.from_dict(json.loads(rsp.content)["dataset"])
    df["name"] = df["c_dataset"].apply(lambda x: "/".join(x))
    df = df.loc[df["c_vintage"].notna()].copy()
    df["year"] = df["c_vintage"].astype(int)
    return df


class Dataset:
    def __init__(self, name: str):
        self.name = self._validate_name(name=name)
        # TODO tests

    @cached_property
    def available_years(self) -> List[int]:
        df = available_datasets()
        return df.loc[df["name"] == self.name]["year"].unique().tolist()
    
    def available_geographies(self, year: int) -> List[str]:
        df = self._load_geographies(year=year)
        return df["name"].tolist()  # TODO harmonize geography names

    def query(
        self,
        year: int,
        geography: str,
        vars: List[str],
        fips_map: Dict[str, str],
        census_api_key: Optional[str] = None,
    ) -> pd.DataFrame:
        rsp = requests.get(
            self._request_url(
                year=year,
                geography=geography,
                vars=vars,
                fips_map=fips_map,
                census_api_key=census_api_key,
            )
        )
        # TODO error handling
        blob_json = rsp.json()
        df = pd.DataFrame.from_dict(blob_json[1:])
        df.columns = blob_json[0]
        return df

    def _request_url(
        self,
        year: int,
        geography: str,
        vars: List[str],
        fips_map: Dict[str, str],
        census_api_key: Optional[str] = None,
    ) -> str:
        if census_api_key is None and CENSUS_API_KEY is None:
            raise LookupError(
                "No census api key found. Please add to your .env or pass directly via `census_api_key`."
            )
        elif census_api_key is None:
            census_api_key = CENSUS_API_KEY
        self._validate_vars(year=year, vars=vars)
        address_base = f"{CENSUS_API_BASE}data/{year}/{self.name}?"
        vars_str = f"NAME,{','.join(vars)}"
        requires, wildcards = self.geography_requires(year=year, geography=geography)
        reqs = list(set(requires) - set(wildcards))
        if any(x not in fips_map.keys() for x in reqs):
            raise ValueError(f"Invalid fips_map. Geography requires {' ,'.join(reqs)}")
        wcs = [x for x in wildcards if x not in fips_map.keys()]
        geo_str = "&".join(
            [f"in={k}:{v}" for k, v in fips_map.items()]
            + [f"in={geo}:*" for geo in wcs]
        )
        key_str = f"key={census_api_key}"
        return f"{address_base}get={vars_str}&for={geography}:*&{geo_str}&{key_str}"

    def geography_requires(
        self, year: int, geography: str
    ) -> Tuple[List[str], List[str]]:
        """returns required geography names and wildcards

        Parameters
        ----------
        year : int
            _description_
        geography : str
            _description_

        Returns
        -------
        Tuple[List[str], List[str]]
            required geographies, required wildcards
        """
        # TODO literal for census geographies
        df = self._load_geographies(year=year)
        df_geo = df.loc[df["name"] == geography].squeeze()
        return df_geo["requires"], df_geo["wildcard"]

    def _load_geographies(self, year: int) -> pd.DataFrame:
        self._validate_year(year=year)
        rsp = requests.get(f"{CENSUS_API_BASE}data/{year}/{self.name}/geography.json")
        df = pd.DataFrame.from_dict(json.loads(rsp.content)["fips"])
        return df.loc[
            df["name"].isin(GEOM_NAME_MAP.values())
        ]  # TODO expand geographies with Tiger expansion

    def variables(self, year: int) -> pd.DataFrame:
        self._validate_year(year=year)
        rsp = requests.get(f"{CENSUS_API_BASE}data/{year}/{self.name}/variables.json")
        df = pd.DataFrame.from_dict(
            json.loads(rsp.content)["variables"], orient="index"
        )[["label", "concept"]].sort_index()
        df = df.loc[~df.index.isin(["for", "in", "ucgid"])]
        df = df.loc[df["concept"].notna()]
        df = df.loc[df["label"] != "Geography"]
        return df

    def _validate_vars(self, year: int, vars: List[str]):
        var_df = self.variables(year=year)
        valid_opts = var_df.index.tolist()
        invalid_vars = [x for x in vars if x not in valid_opts]
        if len(invalid_vars) > 0:
            raise LookupError(
                f"The following vars were not found in valid variable list for {self.name}-{year}: {' ,'.join(invalid_vars)}"
            )

    def _validate_name(self, name: str) -> str:
        if not name in available_datasets()["name"].unique().tolist():
            raise LookupError(
                f"Dataset {name} not recognized. See {CENSUS_API_BASE}data.html for available datasets."
            )
        return name

    def _validate_year(self, year: int):
        if year not in self.available_years:
            raise LookupError(
                f"Year {year} is not valid for {self.name}. Available years are {' ,'.join(self.available_years)}"
            )
