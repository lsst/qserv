
import logging.config
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


def add_logfile_opt(parser):
    """
    Add option to command line interface in order to set path to standar
    configuration file for python logger
    """

    default_log_conf = "{0}/.lsst/logging.yaml".format(os.path.expanduser('~'))
    parser.add_argument("-V", "--log-cfg", dest="log_conf",
                        default=default_log_conf,
                        help="Absolute path to yaml file containing python" +
                        "logger standard configuration file")
    return parser


def setup_logging(path='logging.yaml',
                  default_level=logging.INFO):
    """
    Setup logging configuration from yaml file
    if the yaml file doesn't exists:
    - return false
    - configure logging to default_level
    """
    if os.path.exists(path):
        with open(path, 'r') as f:
            config = yaml.load(f.read())
        logging.config.dictConfig(config)
        return True
    else:
        logging.basicConfig(level=default_level)
        return False


def init_default_logger(log_file_prefix, level=logging.DEBUG, log_path="."):
    if level == logging.DEBUG:
        fmt = '%(asctime)s {%(pathname)s:%(lineno)d} %(levelname)s %(message)s'
    else:
        fmt = '%(asctime)s %(levelname)s %(message)s'
    add_console_logger(level, fmt)
    logger = add_file_logger(log_file_prefix, level, log_path, fmt)
    return logger


def add_console_logger(level=logging.DEBUG, fmt='%(asctime)s %(levelname)s %(message)s'):
    logger = logging.getLogger()
    formatter = logging.Formatter(fmt)
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
