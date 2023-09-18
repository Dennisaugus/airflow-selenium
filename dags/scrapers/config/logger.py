from logging.handlers import TimedRotatingFileHandler
import logging
import sys
import os

# Formatter that specifies the layout of log messages in the final destination
FORMATTER = logging.Formatter(
    "%(asctime)s — %(filename)s:%(lineno)d — [%(levelname)s] — %(message)s"
)


def get_console_handler():
    """
    Handler responsible for sending logs to the terminal (stdout)

    Returns:
        console_handler (logging.StreamHandler): Handler that sends log records (created by loggers) to the console
    """
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(FORMATTER)

    return console_handler


def get_file_handler(logs_file_path):
    """
    Handler responsible for sending logs to the file 'app.log'

    Args:
        logs_file_path (str): The path where the log messages will be saved

    Returns:
        file_handler (logging.handlers.TimedRotatingFileHandler): Handler that sends log records (created by loggers) to the file
    """
    file_handler = TimedRotatingFileHandler(logs_file_path, when="midnight")
    file_handler.setFormatter(FORMATTER)

    return file_handler


def get_logger(logger_name, logs_file_path):
    """
    This function returns the instance of the Logger class that is the entrypoint for the log records

    Args:
        logger_name (str): The name of the logger
        logs_file_path (str): The path where the log messages will be saved

    Returns:
        logger (logging.Logger): Instance of the Logger class
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(get_console_handler())
    logger.addHandler(get_file_handler(logs_file_path))
    logger.propagate = False

    return logger


# Set the logs path
log_dir = os.path.join(os.path.normpath(os.getcwd() + os.sep + os.pardir))
if os.getcwd().split("/")[-1] == "config":
    logs_file_path = os.path.join(log_dir, "logs/app.log")
elif os.getcwd().split("/")[-1] == "aqrl-tactics-pat-dags":
    logs_file_path = os.path.join(log_dir, "aqrl-tactics-pat-dags/dags/logs/app.log")

# LOGGER = get_logger(__name__, logs_file_path)
LOGGER = logging.getLogger('airflow.task')