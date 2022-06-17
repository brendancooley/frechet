"""
tests for loading cartographic boundary files
"""
import pytest
from typing import *

from frechet.fips import State, County
from frechet.tiger import GEOM


def _test_cb(unit: Union[State, County], geom: GEOM, year: int):
    gdf = unit.cb(geom=geom, year=year, cache=True)
    assert len(gdf) > 0


@pytest.mark.parametrize("abbr,year,geom", [(abbr, y, geom) for y in range(2014, 2022) for abbr in ["AL", "CA", "MD"] for geom in get_args(GEOM)])
def test_states(abbr: str, year: int, geom: GEOM):
    st = State.from_abbr(abbr=abbr)
    _test_cb(unit=st, geom=geom, year=year)


@pytest.mark.parametrize("st_abbr,name,year,geom", [("MD", "Montgomery", y, geom) for y in range(2014, 2022) for geom in get_args(GEOM)])
def test_counties(st_abbr: str, name: str, year: int, geom: GEOM):
    co = County.from_state_abbr_name(state_abbr=st_abbr, name=name)
    _test_cb(unit=co, geom=geom, year=year)