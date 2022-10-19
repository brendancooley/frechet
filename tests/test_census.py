import pytest
from frechet.census import Dataset, GEOM_NAME_MAP

# TODO extend to random selections
TEST_DS = "dec/pl"

@pytest.mark.parametrize("geography", list(GEOM_NAME_MAP.values()))
def test_ds(geography):
    fips_map = {"state": "01"}  # TODO test error catching
    ds = Dataset(TEST_DS)
    years = ds.available_years
    for y in years:
        if geography in ds.available_geographies(year=y):
            vars = ds.variables(year=y).index.unique().tolist()[0:5]  #  TODO how are names not harmonized across years???
            df = ds.query(year=y, geography=geography, vars=vars, fips_map=fips_map)
            assert len(df) > 0