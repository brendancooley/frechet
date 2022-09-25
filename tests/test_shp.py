"""
tests for loading cartographic boundary files
"""
import pytest
from typing import *

from frechet.fips import State, County
from frechet.tiger import GEOGRAPHY


def _test_cb(unit: Union[State, County], geom: GEOGRAPHY, year: int):
    gdf = unit.shp(geom=geom, year=year, cache=True, cb=True)
    assert len(gdf) > 0


def _test_shp(unit: Union[State, County], geom: GEOGRAPHY, year: int):
    gdf = unit.shp(geom=geom, year=year, cache=True, cb=False)
    assert len(gdf) > 0


@pytest.mark.parametrize("abbr,year,geom", [(abbr, y, geom) for y in range(2014, 2022) for abbr in ["AL", "CA", "MD"] for geom in get_args(GEOGRAPHY)])
def test_states(abbr: str, year: int, geom: GEOGRAPHY):
    st = State.from_abbr(abbr=abbr)
    _test_cb(unit=st, geom=geom, year=year)


@pytest.mark.parametrize("st_abbr,name,year,geom", [("MD", "Montgomery", y, geom) for y in range(2014, 2022) for geom in get_args(GEOGRAPHY)])
def test_counties(st_abbr: str, name: str, year: int, geom: GEOGRAPHY):
    co = County.from_state_abbr_name(state_abbr=st_abbr, name=name)
    _test_cb(unit=co, geom=geom, year=year)


def test_tl():
    """Test finding TIGER line files for specific county"""
    co = County.from_state_abbr_name(state_abbr="MD", name="Montgomery")
    for geom in get_args(GEOGRAPHY):
        _test_shp(unit=co, geom=geom, year=2015)
        _test_shp(unit=co, geom=geom, year=2021)  # different file structures after 2020