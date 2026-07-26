import os
from pathlib import Path
from dotenv import load_dotenv

# 1. Dynamically locate the project root directory
# (This points to breeze-live-pipeline/ no matter what machine runs it)
BASE_DIR = Path(__file__).resolve().parent.parent

# 2. Explicitly load the raw .env file text into the system environment
ENV_FILE_PATH = BASE_DIR / ".env"
load_dotenv(dotenv_path=ENV_FILE_PATH)

class Settings:
    """Safely translates raw environment strings into structured Python primitives."""
    
    # --- Infrastructure Paths ---
    BASE_DIR: Path = BASE_DIR
    DB_DIR: Path = BASE_DIR / "data"
    DB_PATH: Path = DB_DIR / "pipeline.db"
    
    # --- Breeze API Authentication (Private Vault) ---
    BREEZE_API_KEY: str = os.getenv("BREEZE_API_KEY", "")
    BREEZE_SECRET_KEY: str = os.getenv("BREEZE_SECRET_KEY", "")
    
    # --- Application Constants & Stream Constraints ---
    DEFAULT_EXCHANGE: str = "NSE"
    # Feel free to add tracking assets or performance configurations here later
    
    def __init__(self):
        # Fail-fast validation check for developer environment
        if not self.BREEZE_API_KEY or not self.BREEZE_SECRET_KEY:
            raise ValueError(
                "CRITICAL ERROR: BREEZE_API_KEY or BREEZE_SECRET_KEY is missing from your .env file.\n"
                "Please verify that your root .env file exists and contains valid credentials."
            )

# Instantiate a single object for global import across the system
settings = Settings()