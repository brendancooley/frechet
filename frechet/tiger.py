import os
import logging
from pathlib import Path
from typing import *
import shutil

import geopandas as gpd

from frechet.url import TIGER_BASE
from frechet.util import unzip_to_tmp, cache_result_dir, RESULT_DIR
from frechet.settings import FRECHET_CACHE_DIR


GEOM = Literal['tracts', 'block_groups', 'county_sub']
GEOM_MAP = {
    "tracts": "tract",
    "block_groups": "bg",
    "county_sub": "cousub"
}


class ShpNotFound(Exception):
    ...


def load_shp(year: int, st_fips: str, geom: GEOM, cache: bool = False):
    """
    Load cartographic boundary files for st-geom-year. If cache=True, save the results to FRECHET_CACHE_DIR.

    Args:
        year (int): year of boundary file
        st_fips (str): state fips code for boundary file
        geom (frechet.tiger.GEOM): type of geometries to request, tracts, block_groups, or county subdivisions
        cache (bool): If True, save results to FRECHET_CACHE_DIR

    Returns:
        geopandas.DataFrame:
    """
    if year < 2014:
        raise ValueError("Tiger loads for years prior to 2014 not yet implemented.")
    subpath = f"GENZ{year}/shp/cb_{year}_{st_fips}_{GEOM_MAP[geom]}_500k"
    fname = f"cb_{year}_{st_fips}_{GEOM_MAP[geom]}_500k.shp"
    if FRECHET_CACHE_DIR is not None:
        local_shp_path = Path(FRECHET_CACHE_DIR) / Path(subpath) / fname
        if os.path.isfile(os.path.expanduser(local_shp_path)):
            logging.info(f"Loading shp from local cache at {local_shp_path}")
            return gpd.read_file(local_shp_path)
    url = TIGER_BASE + subpath + ".zip"
    zip_found = unzip_to_tmp(url=url)
    if zip_found:
        gdf = gpd.read_file(RESULT_DIR + f"/{fname}")
        if cache:
            if FRECHET_CACHE_DIR is None:
                raise ValueError("Attempting to cache download without setting FRECHET_CACHE_DIR. Please add to .env.")
            logging.info(f"Caching results to {Path(FRECHET_CACHE_DIR) / Path(subpath)}")
            cache_result_dir(subdir=subpath)
        shutil.rmtree(RESULT_DIR)
        return gdf
    else:
        raise ShpNotFound(f"No boundary files found at {url}")
