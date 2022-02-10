import os

from utils.logger import setup_logger

os.makedirs("output", exist_ok=True)
# Setup logger file
logger = setup_logger()
