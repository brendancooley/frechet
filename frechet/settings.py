import os
from dotenv import load_dotenv

load_dotenv()

FRECHET_CACHE_DIR = os.getenv("FRECHET_CACHE_DIR")