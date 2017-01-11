# -*- coding: utf-8 -*-
"""
Created on 17-10-2016
@author: Connah Cutbush
Basic logging functions
"""

import logging.handlers
import os
import platform

import logging
import settings

LAST_MODULE = None


def get_log_path(module):
    log_file = os.path.join(settings.LOG_PATH, '%s.log' % module)

    platform_name = platform.uname()[0]

    if platform_name == 'Darwin':
        if not os.path.isdir(settings.LOG_PATH_MAC):
            os.makedirs(settings.LOG_PATH_MAC)
        log_file = os.path.join(settings.LOG_PATH_MAC, '%s.log' % module)
    elif platform_name == 'Windows':
        if not os.path.isdir(settings.LOG_PATH):
            try:
                os.makedirs(settings.LOG_PATH)
            except (FileNotFoundError, PermissionError):
                # try C: drive
                if not os.path.isdir(settings.LOG_PATH_C):
                    os.makedirs(settings.LOG_PATH_C)
                log_file = os.path.join(settings.LOG_PATH_C, '%s.log' % module)
    return log_file


def get_logger(module=None, loglevel=logging.INFO):
    global LAST_MODULE
    # use last module
    if module is None and LAST_MODULE:
        module = LAST_MODULE
    if module is None:
        module = 'default'  # default
    logger = logging.getLogger(module)
    LAST_MODULE = module
    logger.propagate = False

    if not getattr(logger, 'handler_set', None):
        consolehandler = logging.StreamHandler()

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        consolehandler.setFormatter(formatter)
        logger.addHandler(consolehandler)

        # create a file handler
        log_path = get_log_path(module)
        handler = logging.handlers.TimedRotatingFileHandler(log_path, 'D', 1, 5)

        # create a logging format
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)

        # add the handlers to the logger
        logger.addHandler(handler)

        logger.setLevel(loglevel)
        logger.handler_set = True
    return logger
