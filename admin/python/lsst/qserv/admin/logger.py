import logging
import os 

def init_default_logger(log_file_prefix, level=logging.DEBUG, log_path="."):
    if level == logging.DEBUG:
        format = '%(asctime)s {%(pathname)s:%(lineno)d} %(levelname)s %(message)s'
    else:
        format = '%(asctime)s %(levelname)s %(message)s'
    add_console_logger(level, format)
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
    logfile = os.path.join(log_path,log_file_prefix+'.log')
    file_handler = logging.FileHandler(logfile)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger
