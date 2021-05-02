# -*- coding:utf-8 -*-
import logging
import os
import colorlog
from logging.handlers import RotatingFileHandler
from datetime import datetime

CUR_PATH = os.path.dirname(os.path.realpath("."))  # 当前项目路径
LOG_PATH = os.path.join(CUR_PATH, 'logs')  # log_path为存放日志的路径
if not os.path.exists(LOG_PATH): os.mkdir(LOG_PATH)  # 若不存在logs文件夹，则自动创建

LOG_COLOR_CONFIG = {
    # 终端输出日志颜色配置
    'DEBUG': 'white',
    'INFO': 'cyan',
    'WARNING': 'yellow',
    'ERROR': 'red',
    'CRITICAL': 'bold_red',
}

DEFAULT_FORMART = {
    # 终端输出格式
    'color_format': '%(log_color)s%(asctime)s-%(name)s-%(levelname)s-[日志信息]: %(message)s',
    # 日志输出格式
    'log_format': '>>> %(asctime)s-%(name)s-%(levelname)s-[日志信息]: %(message)s'
}


class HandleLog:
    """
    先创建日志记录器（logging.getLogger），然后再设置日志级别（logger.setLevel），
    接着再创建日志文件，也就是日志保存的地方（logging.FileHandler），然后再设置日志格式（logging.Formatter），
    最后再将日志处理程序记录到记录器（addHandler）
    """

    def __init__(self, task_name):
        log_path = f"{LOG_PATH}/{task_name}"
        if not os.path.exists(log_path):
            os.makedirs(log_path)
        self.__now_time = datetime.now().strftime('%Y-%m-%d')  # 当前日期格式化
        self.__log_path = os.path.join(log_path, f"{self.__now_time}.log")  # 收集所有日志信息文件
        self.__log_error_path = os.path.join(log_path, f"{self.__now_time}-error.log")
        self.__logger = logging.getLogger()  # 创建日志记录器
        self.__logger.setLevel(logging.DEBUG)  # 设置默认日志记录器记录级别

    @staticmethod
    def __init_logger_handler(log_path):
        """
        创建日志记录器handler，用于收集日志
        :param log_path: 日志文件路径
        :return: 日志记录器
        """
        # 写入文件，如果文件超过1M大小时，切割日志文件，仅保留3个文件
        logger_handler = RotatingFileHandler(filename=log_path, maxBytes=10 * 1024 * 1024, backupCount=3,
                                             encoding='utf-8')
        return logger_handler

    @staticmethod
    def __init_console_handle():
        """创建终端日志记录器handler，用于输出到控制台"""
        console_handle = colorlog.StreamHandler()
        return console_handle

    def __set_log_handler(self, logger_handler, level=logging.DEBUG):
        """
        设置handler级别并添加到logger收集器
        :param logger_handler: 日志记录器
        :param level: 日志记录器级别
        """
        logger_handler.setLevel(level=level)
        self.__logger.addHandler(logger_handler)

    def __set_color_handle(self, console_handle):
        """
        设置handler级别并添加到终端logger收集器
        :param console_handle: 终端日志记录器
        :param level: 日志记录器级别
        """
        console_handle.setLevel(logging.DEBUG)
        self.__logger.addHandler(console_handle)

    @staticmethod
    def __set_color_formatter(console_handle, color_config):
        """
        设置输出格式-控制台
        :param console_handle: 终端日志记录器
        :param color_config: 控制台打印颜色配置信息
        :return:
        """
        formatter = colorlog.ColoredFormatter(DEFAULT_FORMART["color_format"], log_colors=color_config)
        console_handle.setFormatter(formatter)

    @staticmethod
    def __set_log_formatter(file_handler):
        """
        设置日志输出格式-日志文件
        :param file_handler: 日志记录器
        """
        formatter = logging.Formatter(DEFAULT_FORMART["log_format"], datefmt='%a, %d %b %Y %H:%M:%S')
        file_handler.setFormatter(formatter)

    @staticmethod
    def __close_handler(file_handler):
        """
        关闭handler
        :param file_handler: 日志记录器
        """
        file_handler.close()

    def __console(self, level, message):
        """构造日志收集器"""
        logger_handler = self.__init_logger_handler(self.__log_path)  # 创建日志文件
        error_logger_handler = self.__init_logger_handler(self.__log_error_path)  # 创建日志文件
        console_handle = self.__init_console_handle()

        self.__set_log_formatter(logger_handler)  # 设置日志格式
        self.__set_log_formatter(error_logger_handler)  # 设置日志格式
        self.__set_color_formatter(console_handle, LOG_COLOR_CONFIG)

        self.__set_log_handler(logger_handler)  # 设置handler级别并添加到logger收集器
        self.__set_log_handler(error_logger_handler, level=logging.ERROR)  # 设置handler级别并添加到logger收集器
        self.__set_color_handle(console_handle)

        if level == 'info':
            self.__logger.info(message)
        elif level == 'debug':
            self.__logger.debug(message)
        elif level == 'warning':
            self.__logger.warning(message)
        elif level == 'error':
            self.__logger.error(message, exc_info=True)
        elif level == 'critical':
            self.__logger.critical(message)

        self.__logger.removeHandler(logger_handler)  # 避免日志输出重复问题
        self.__logger.removeHandler(error_logger_handler)  # 避免日志输出重复问题
        self.__logger.removeHandler(console_handle)

        self.__close_handler(logger_handler)  # 关闭handler

    def debug(self, message):
        self.__console('debug', message)

    def info(self, message):
        self.__console('info', message)

    def warning(self, message):
        self.__console('warning', message)

    def error(self, message):
        self.__console('error', message)

    def critical(self, message):
        self.__console('critical', message)


if __name__ == '__main__':
    log = HandleLog("test")
    log.info("这是日志信息")
    log.debug("这是debug信息")
    log.warning("这是警告信息")
    log.error("这是错误日志信息")
    log.critical("这是严重级别信息")