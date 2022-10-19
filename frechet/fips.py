from functools import lru_cache
from re import sub
from typing import *
from dataclasses import dataclass
from urllib import request
import pandas as pd
import geopandas as gpd
import requests

from frechet.url import STATES
from frechet.tiger import load_shp
from frechet.geom import GEOGRAPHY, PARENT
from frechet.settings import CENSUS_API_KEY

QUERY_MODE = Literal["name", "abbr", "fips"]


class StateNotFound(Exception):
    ...


class CountyNotFound(Exception):
    ...


class MultipleCountiesError(Exception):
    ...


@lru_cache()
def _load_states() -> pd.DataFrame:
    return pd.read_csv(STATES, delimiter="|", dtype={"STATE": str})


@lru_cache()
def _load_counties(st_fips: str, st_abbr: str) -> pd.DataFrame:
    co_df = pd.read_csv(
        f"https://www2.census.gov/geo/docs/reference/codes/files/st{st_fips}_{st_abbr.lower()}_cou.txt",
        header=None,
        dtype=str,
    )
    co_df.columns = ["state_abbr", "state_fips", "co_fips", "co_name", "co_type"]
    return co_df


def _build_state(mode: QUERY_MODE, name: str):
    if type(name) != str:
        raise ValueError(f"Argument {mode} must be of type `str`.")
    st_df = _load_states()
    col = "STATE_NAME" if mode == "name" else "STUSAB" if mode == "abbr" else "STATE"
    st_row = st_df.loc[st_df[col] == name]
    if len(st_row) == 0:
        raise StateNotFound(
            f"State with {mode} {name} not found. Check {STATES} for available states."
        )
    elif len(st_row) > 1:
        raise ValueError(f"Multiple states with {mode} {name} found.")
    else:
        st_srs = st_row.iloc[0]
        return State(fips=st_srs.STATE, abbr=st_srs.STUSAB, name=st_srs.STATE_NAME)


@dataclass
class State:
    """
    A U.S. State with it's fips code, abbreviation, and name stored as attributes.

    Args:
        fips (str): the state's FIPS code
        abbr (str): the state's abbreviation
        name (str): the state's name

    """

    fips: str
    abbr: str
    name: str

    @classmethod
    def from_name(cls, name: str) -> "State":
        """
        Args:
            name (str): the state's name, e.g. "Alabama"

        Returns:
            State
        """
        return _build_state(mode="name", name=name)

    @classmethod
    def from_abbr(cls, abbr: str) -> "State":
        """

        Args:
            abbr (str): the state's abbreviation, e.g. "AL"

        Returns:
            State
        """
        return _build_state(mode="abbr", name=abbr)

    @classmethod
    def from_fips(cls, fips: str) -> "State":
        """

        Args:
            abbr (str): the state's fips code, e.g. "01"

        Returns:
            State
        """
        return _build_state(mode="fips", name=fips)

    @property
    def county_df(self) -> pd.DataFrame:
        """

        Returns:
            pd.DataFrame: a table storing information (fips codes and names) of counties within the state.

        """
        co_df = _load_counties(st_fips=self.fips, st_abbr=self.abbr)
        return co_df

    @property
    def counties(self) -> List:
        """

        Returns:
            List: a list of county names within the state

        """
        return self.county_df["co_name"].tolist()

    def shp(
        self, geom: GEOGRAPHY, year: int, cache: bool = False, cb: bool = False
    ) -> gpd.GeoDataFrame:
        """
        returns the state's cartographic boundary files for the geom-year

        Args:
            geom (frechet.tiger.GEOM): a set of geographies to return
            year (int): the year for which to return the geographies
            cache (bool): if True, cache the result
            cb (bool): if True, return the cartographic boundary (less detailed, more efficient) shps

        Returns:
            geopandas.GeoDataFrame: A cartographic boundary geo data frame for the state
        """
        return load_shp(st_fips=self.fips, geom=geom, year=year, cache=cache, cb=cb)


@dataclass
class County:
    """
    A U.S. County with it's fips code, name, and parent `State` stored as attributes.

    Args:
        fips (str): the county's FIPS code
        name (str): the county's name
        state (State): the state in which the county resides

    """

    fips: str
    name: str
    state: State

    @classmethod
    def from_state_abbr_name(cls, state_abbr: str, name: str) -> "County":
        """

        Args:
            state_abbr: two letter abbreviation of state in which county resides (e.g. "MD")
            name: name, or beginning of name, of county (e.g. Montgomery)

        Returns:
            County
        """
        state = State.from_abbr(state_abbr)
        matches = [x for x in state.counties if x.lower().startswith(name.lower())]
        if len(matches) > 1:
            raise MultipleCountiesError(
                f"Multiple counties matching {name} found in {state.name}: {', '.join(matches)}"
            )
        elif len(matches) == 0:
            raise CountyNotFound(f"County {name} not found in {state.name}.")
        else:
            co_srs = state.county_df.loc[
                state.county_df["co_name"].str.startswith(name)
            ].iloc[0]
            return cls(
                fips=co_srs.co_fips,
                name=co_srs.co_name,
                state=state,
            )

    def shp(
        self, geom: GEOGRAPHY, year: int, cache: bool = False, cb: bool = False
    ) -> gpd.GeoDataFrame:
        """
        returns the county's cartographic boundary files for the geom-year

        Args:
            geom (frechet.tiger.GEOM): a set of geographies to return
            year (int): the year for which to return the geographies
            cache (bool): if True, cache the result
            cb (bool): if True, return the cartographic boundary (less detailed, more efficient) shps

        Returns:
            geopandas.GeoDataFrame: A cartographic boundary geo data frame for the state
        """
        st_gdf = self.state.shp(geom=geom, year=year, cache=cache, cb=cb)
        return st_gdf.loc[st_gdf["COUNTYFP"] == self.fips]
