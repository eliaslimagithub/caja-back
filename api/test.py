import logging
import logging.config
import configparser
import sys

config_file = '../settings/default.ini'
config = configparser.ConfigParser()
config.read(config_file)

logging.config.fileConfig(config_file)


# Llamada para configurar logging

# Ejemplo de uso
logger = logging.getLogger("DDDD")
logger.debug("Este es un mensaje DEBUG")
logger.info("Este es un mensaje INFO")
logger.warning("Este es un mensaje WARNING")
logger.error("Este es un mensaje ERROR")
logger.critical("Este es un mensaje CRITICAL")