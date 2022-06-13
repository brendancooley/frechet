import os
import requests
import zipfile
import shutil
from pathlib import Path
from dotenv import load_dotenv

RESULT_DIR = '/tmp/results'


def unzip_to_tmp(url: str):
    """
    extracts contents of zipfile stored at `url` to 'tmp/results'

    Args:
        url: location of zipfile

    Returns:

    """
    results = requests.get(url)

    with open('/tmp/zip_folder.zip', 'wb') as f:
        f.write(results.content)

    file = zipfile.ZipFile('/tmp/zip_folder.zip')
    file.extractall(path=RESULT_DIR)


def save_to_cache(url: str, subdir: str):
    """

    Args:
        url: location of zipfile
        subdir: local subdirectory for saving cached results (parent directory is

    Returns:

    """
    # TODO different path structure for windows?
    unzip_to_tmp(url)
    load_dotenv()
    cache_dir = os.getenv("FRECHET_CACHE_DIR")
    output_dir = Path(os.path.expanduser(Path(cache_dir) / Path(subdir)))
    files = os.listdir(RESULT_DIR)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    for file in files:
        os.rename(Path(RESULT_DIR) / file, output_dir / file)
    shutil.rmtree(RESULT_DIR)