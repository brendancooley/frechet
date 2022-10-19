import os
import logging
from pathlib import Path
from typing import *
import shutil

import geopandas as gpd

from frechet.geom import GEOGRAPHY
from frechet.url import TIGER_BASE
from frechet.util import unzip_to_tmp, cache_result_dir, RESULT_DIR
from frechet.settings import FRECHET_CACHE_DIR


GEOM_MAP: Dict[GEOGRAPHY, str] = {
    "tracts": "tract",
    "block_groups": "bg",
    "county_sub": "cousub",
    "blocks": "tabblock"
}
CB_GEOM = Literal["tracts", "block_groups", "county_sub"]  # geoms for which cartographic boundary files are available


class ShpNotFound(Exception):
    ...


class CBUnavailable(Exception):
    ...


# TODO move tiger stuff onto rest API
# https://github.com/nkrishnaswami/uscensus/blob/master/GetCountyShapes.ipynb
def load_shp(year: int, st_fips: str, geom: GEOGRAPHY, cache: bool = False, cb: bool = False):
    """
    Load cartographic boundary files for st-geom-year. If cache=True, save the results to FRECHET_CACHE_DIR.

    Args:
        year (int): year of boundary file
        st_fips (str): state fips code for boundary file
        geom (frechet.tiger.GEOM): type of geometries to request, tracts, block_groups, or county subdivisions
        cache (bool): If True, save results to FRECHET_CACHE_DIR
        cb (bool): if True, return the cartographic boundary (less detailed, more efficient) shps

    Returns:
        geopandas.DataFrame:
    """
    if year < 2014:
        raise ValueError("Tiger loads for years prior to 2014 not yet implemented.")
    _validate_cb(geom=geom, cb=cb)
    subpath, fname = _fpath(year=year, st_fips=st_fips, geom=geom, cb=cb)
    if FRECHET_CACHE_DIR is not None:
        local_shp_path = Path(FRECHET_CACHE_DIR) / Path(subpath) / fname
        if os.path.isfile(os.path.expanduser(local_shp_path)):
            logging.info(f"Loading shp from local cache at {local_shp_path}")
            return _load_tiger(local_shp_path)
    url = TIGER_BASE + subpath + ".zip"
    zip_found = unzip_to_tmp(url=url)
    if zip_found:
        gdf = _load_tiger(RESULT_DIR + f"/{fname}")
        if cache:
            if FRECHET_CACHE_DIR is None:
                raise ValueError("Attempting to cache download without setting FRECHET_CACHE_DIR. Please add to .env.")
            logging.info(f"Caching results to {Path(FRECHET_CACHE_DIR) / Path(subpath)}")
            cache_result_dir(subdir=subpath)
        shutil.rmtree(RESULT_DIR)
        return gdf
    else:
        raise ShpNotFound(f"No boundary files found at {url}")


def _load_tiger(path: str) -> gpd.GeoDataFrame:
    gdf = gpd.read_file(path)
    gdf.columns = [x[:-2] if x.endswith("10") or x.endswith("20") else x for x in gdf.columns]
    return gdf


def _fpath(year: int, st_fips: str, geom: GEOGRAPHY, cb: bool) -> Tuple[str, str]:
    if cb:
        subpath = f"GENZ{year}/shp/cb_{year}_{st_fips}_{GEOM_MAP[geom]}_500k"
        fname = f"cb_{year}_{st_fips}_{GEOM_MAP[geom]}_500k.shp"
    else:
        if geom == "blocks":
            fsfx = "10" if year < 2020 else "20"
            gsfx = "" if year < 2020 else "20"
        else:
            fsfx = ""
            gsfx = ""
        subpath = f"TIGER{year}/{GEOM_MAP[geom].upper()}{gsfx}/tl_{year}_{st_fips}_{GEOM_MAP[geom]}{fsfx}"
        fname = f"tl_{year}_{st_fips}_{GEOM_MAP[geom]}{fsfx}.shp"
    return subpath, fname


def _validate_cb(geom: GEOGRAPHY, cb: bool):
    if not cb:
        return True
    else:
        if geom in get_args(CB_GEOM):
            return True
        else:
            raise CBUnavailable(f"Cartographic boundary files unavailable for geom {geom}")