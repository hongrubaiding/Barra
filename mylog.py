# -- coding: utf-8 --
# Author:zouhao
# email:1084848158@qq.com

# 日志
import logging


def set_log(file_name=''):
    logger = logging.getLogger()
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)

        formatter = logging.Formatter("%(asctime)s %(filename)s:%(levelname)s:%(message)s", datefmt="%Y-%m-%d %H:%M:%S")
        ch.setFormatter(formatter)
        logger.addHandler(ch)

    # logger.setLevel(logging.INFO)
    # ch = logging.StreamHandler()
    # ch.setLevel(logging.INFO)
    #
    formatter = logging.Formatter("%(asctime)s %(filename)s:%(levelname)s:%(message)s", datefmt="%Y-%m-%d  %H:%M:%S")
    # ch.setFormatter(formatter)
    # logger.addHandler(ch)

    if file_name:
        file_handler = logging.FileHandler('%s.log' % file_name,mode="w+")
        file_handler.setLevel(level=logging.INFO)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    return logger
