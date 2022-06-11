from frechet.fips import State, QUERY_MODE, StateNotFound


def _test_state_init(mode: QUERY_MODE, query: str):
    if mode == "abbr":
        return State.from_abbr(abbr=query)
    elif mode == "name":
        return State.from_name(name=query)
    elif mode == "fips":
        return State.from_fips(fips=query)
    else:
        raise ValueError(f"Query mode {mode} not implemented.")


def test_state_init():
    state_abbr = _test_state_init(mode="abbr", query="AL")
    state_name = _test_state_init(mode="name", query="Alabama")
    state_fips = _test_state_init(mode="fips", query="01")
    assert state_abbr.fips == state_name.fips == state_fips.fips == "01"
    try:
        state_invalid = _test_state_init(mode="name", query="China")
    except StateNotFound:
        pass