from functools import lru_cache
from typing import *
from dataclasses import dataclass
import pandas as pd

from frechet.url import STATES

QUERY_MODE = Literal["name", "abbr", "fips"]


class StateNotFound(Exception):
    ...


@lru_cache()
def load_states():
    return pd.read_csv(STATES, delimiter="|", dtype={"STATE": str})


def build_state(mode: QUERY_MODE, name: str):
    if type(name) != str:
        raise ValueError(f"Argument {mode} must be of type `str`.")
    st_df = load_states()
    col = "STATE_NAME" if mode == "name" else "STUSAB" if mode == "abbr" else "STATE"
    st_row = st_df.loc[st_df[col] == name]
    if len(st_row) == 0:
        raise StateNotFound(f"State with {mode} {name} not found. Check {STATES} for available states.")
    elif len(st_row) > 1:
        raise ValueError(f"Multiple states with {mode} {name} found.")
    else:
        st_srs = st_row.iloc[0]
        return State(
            fips=st_srs.STATE,
            abbr=st_srs.STUSAB,
            name=name
        )


@dataclass
class State:
    fips: str
    abbr: str
    name: str

    @classmethod
    def from_name(cls, name: str):
        return build_state(mode="name", name=name)

    @classmethod
    def from_abbr(cls, abbr: str):
        return build_state(mode="abbr", name=abbr)

    @classmethod
    def from_fips(cls, fips: str):
        return build_state(mode="fips", name=fips)
