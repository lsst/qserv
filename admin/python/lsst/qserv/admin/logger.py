import logging
import os
import yaml

# Used to parse cli logging values
verbose_dict = {
    'DEBUG': logging.DEBUG,
    'INFO': logging.INFO,
    'WARNING': logging.WARNING,
    'ERROR': logging.ERROR,
    'FATAL': logging.FATAL,
}


def setup_logging(default_path='logging.yaml',
                  default_level=logging.INFO,
                  env_key='LOG_CFG'):
    """
    Setup logging configuration from yaml file
    """
    path = default_path
    value = os.getenv(env_key, None)
    if value:
        path = value
    if os.path.exists(path):
        with open(path, 'rt') as f:
            config = yaml.load(f.read())
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)


def init_default_logger(log_file_prefix, level=logging.DEBUG, log_path="."):
    if level == logging.DEBUG:
        _format = '%(asctime)s {%(pathname)s:%(lineno)d} %(levelname)s %(message)s'
    else:
        _format = '%(asctime)s %(levelname)s %(message)s'
    add_console_logger(level, _format)
    logger = add_file_logger(log_file_prefix, level, log_path, format)
    return logger


def add_console_logger(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s'):
    logger = logging.getLogger()
    formatter = logging.Formatter(format)
    logger.setLevel(level)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger


def add_file_logger(log_file_prefix, level=logging.DEBUG, log_path=".", format='%(asctime)s %(levelname)s %(message)s'):

    logger = logging.getLogger()
    formatter = logging.Formatter(format)
    # this level can be reduce for each handler
    logger.setLevel(level)
    logfile = os.path.join(log_path, log_file_prefix + '.log')
    file_handler = logging.FileHandler(logfile)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger
