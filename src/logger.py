# kafka_logger/logger.py
import logging

# Configure the logging settings
logging.basicConfig(
    filename='delivery_reports.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Define a logger for the package
logger = logging.getLogger(__name__)