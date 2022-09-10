import os
from dotenv import load_dotenv

load_dotenv()  # TODO: test .env finding outside context of frechet module

FRECHET_CACHE_DIR = os.getenv("FRECHET_CACHE_DIR")
CENSUS_API_KEY = os.getenv("CENSUS_API_KEY")