from enum import Enum
import datetime


class LogLevel(Enum):
    DEBUG = 1
    INFO = 2
    WARN = 3
    ERROR = 4
    FATAL = 5

    def __str__(self):
        levels = ["?", "D", "I", "W", "E", "F"]
        return levels[self.value]


class Logger:
    def __init__(self, tag="Logger", log_level=LogLevel.INFO):
        self.__log_level = log_level
        self.__tag = tag

    def __message(self, level, msg):
        if level.value >= self.__log_level.value:
            curr_time = datetime.datetime.now()
            print("[%s] [%s] %s" % (curr_time.strftime("%H:%M:%S"), str(level) + "/" + self.__tag, str(msg)))

    def set_verbosity(self, log_level: LogLevel):
        self.__log_level = log_level

    def d(self, message):
        self.__message(LogLevel.DEBUG, message)

    def i(self, message):
        self.__message(LogLevel.INFO, message)

    def w(self, message):
        self.__message(LogLevel.WARN, message)

    def e(self, message):
        self.__message(LogLevel.ERROR, message)

    def f(self, message):
        self.__message(LogLevel.FATAL, message)
