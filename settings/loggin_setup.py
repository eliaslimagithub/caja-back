import logging
import logging.config
import os
import datetime
import sys

def setup_logging(config_file='../settings/default.ini'):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(base_dir, 'default.ini')

    logging.config.fileConfig(config_path, disable_existing_loggers=False)

    os.makedirs('../api/logs', exist_ok=True)
    log_filename = f"logs/app_{datetime.date.today()}.log"

    file_handler = logging.FileHandler(log_filename, mode='a', encoding='utf-8')
    file_handler.setLevel(logging.INFO)

    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(name)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(formatter)

    logging.getLogger().addHandler(file_handler)


#setup_logging()
#logger = logging.getLogger(__name__)
#logger.info("Inicio del programa")