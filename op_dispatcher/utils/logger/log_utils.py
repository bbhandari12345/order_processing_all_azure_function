
"""This module deals with definition of logging utils, where one can fetch a
logger and use it for log record.

Configuration for the logger should be defined anywhere but should
be provided upon instantiation of LogUtils.

As of now we only support two loggers:

`nativelogger`: Logs to console and rotating files inside provided log directory.
                For nativelogger a filename must be provided so that logs get seggrate by
                module.

`log2cloudwatch`: Logs to AWS cloudwatch
"""
from typing import Callable
import os
import logging
from logging.handlers import TimedRotatingFileHandler
from functools import wraps
import time


LOG_LEVELS = {

    'CRITICAL': logging.CRITICAL,
    'ERROR': logging.ERROR,
    'WARNING': logging.WARNING,
    'WARN': logging.WARNING,
    'INFO': logging.INFO,
    'DEBUG': logging.DEBUG,
    'NOTSET': logging.NOTSET
}


class LogUtils:
    """This module deals with definition of logging utils, where one can fetch
    a logger and use it for log record.

    Configuration for the logger should be defined anywhere but should
    be provided upon instantiation of LogUtils.

    As of now we only support two loggers:

    `nativelogger`: Logs to console and rotating files inside provided log directory.
                    For nativelogger a filename must be provided so that logs get seggrate by
                    module.

    `log2cloudwatch`: Logs to AWS cloudwatch

    :param log_config: A dictionary for logger configuration.
                            Configuration should be strictly followed
                            as below.
    'nativelogger':{
                            'filehandler':{
                                'maxbytes':10485760,
                                'maxbackupcount':5,
                                'logrotate':{
                                    'when':'h',
                                    'at':1,
                                    'utc':False
                                },
                                'logdir':'/home/logs',
                                'level':'DEBUG',
                                'format':"%(levelname)s [%(asctime)s] %(filename)s %(funcName)s
                                         : %(message)s"
                            },
                            'consolehandler':{
                                'level':'INFO',
                                'format':"%(levelname)s [%(asctime)s] %(filename)s
                                         %(funcName)s : %(message)s"
                            }
                        },
                        'log2cloudwatch':{
                            'level':'DEBUG',
                            'format':"%(levelname)s [%(asctime)s] %(filename)s
                                     %(funcName)s : %(message)s"
                        }

                    }

    :type log_config: dict
    """

    NATIVE_LOGGER = 'nativelogger'

    def __init__(self, log_config: dict) -> None:
        """Constructor for Logutils.

        :param log_config: A dictionary for logger configuration.
                            Configuration should be strictly followed
                            as below.

            'nativelogger':{
                            'filehandler':{
                                'maxbytes':10485760,
                                'maxbackupcount':5,
                                'logrotate':{
                                    'when':'h',
                                    'at':1,
                                    'utc':False
                                },
                                'logdir':'/home/logs',
                                'level':'DEBUG',
                                'format':"%(levelname)s [%(asctime)s] %(filename)s %(funcName)s
                                         : %(message)s"
                            },
                            'consolehandler':{
                                'level':'INFO',
                                'format':"%(levelname)s [%(asctime)s] %(filename)s
                                         %(funcName)s : %(message)s"
                            }
                        },
                        'log2cloudwatch':{
                            'level':'DEBUG',
                            'format':"%(levelname)s [%(asctime)s] %(filename)s
                                     %(funcName)s : %(message)s"
                        }

                    }

        :type log_config: dict
        """
        self.log_config = log_config
        self.native_file_handler_config = self.log_config.get(
            LogUtils.NATIVE_LOGGER).get('filehandler')  # type: ignore
        self.filehandler_rotation_config = self.native_file_handler_config.get(
            'logrotate')  # type: ignore
        self.native_console_handler_config = self.log_config.get(
            LogUtils.NATIVE_LOGGER).get('consolehandler')  # type: ignore

    def get_logger(self, logger_identifier: str, filename: str) -> logging.Logger:
        """Provides loggers for logging with appropriate log handlers. As of
        now we only support one logger:

        `nativelogger`: Logs to console and rotating files inside provided log directory.
                        For nativelogger a filename must be provided so that logs get segregated by
                        module.


        :param logger_identifier: logger to get either `nativelogger` or `log2cloudwatch`
        :type logger_identifier: str

        :pararm filename: log will be saved for the module as {filename}.log
                            where filename can be any str
                            but its good practice to set filename as module filename
                             i.e. filename = flask_api.py
                        While using `cloudwatch` logger *stream name will be set to filename*.
        :type filename: str

        :return: Pyhton logging module logger.
        :rtype: logging.Logger

        :raises ValueError: 1. Raised when filename isn't provided while trying to get `nativelogger`
                            2. Raised when logger_identifier isn't recognized i.e.
                                other then `nativelogger`, `log2cloudwatch`.
        """

        if logger_identifier == LogUtils.NATIVE_LOGGER:

            if filename is None:
                raise ValueError(
                    f'Filename must be provided for {logger_identifier}')

            # checking for log dir        logger = None
            logs_directory = self.native_file_handler_config.get('logdir')
            if not os.path.exists(logs_directory):
                os.mkdir(logs_directory)

            # Getting new Logger everytime instead of getLogger (returns cached logger)
            # This will prevent multiple handlers to be assigned to the same logger
            # This could make this function call thread safe. (Not sure. Investigation is required.)
            logger = logging.Logger(LogUtils.NATIVE_LOGGER)

            # handlers
            log_file = os.path.join(logs_directory, filename) + '.log'

            file_handler = SizeAndTimeRotatingFileHandler(log_file,
                                                          maxBytes=self.native_file_handler_config.get(
                                                              'maxbytes'),
                                                          backupCount=self.native_file_handler_config.get(
                                                              'maxbackupcount'),
                                                          when=self.filehandler_rotation_config.get(
                                                              'when'),
                                                          interval=self.filehandler_rotation_config.get(
                                                              'interval'),
                                                          utc=self.filehandler_rotation_config.get(
                                                              'utc')

                                                          )
            console_handler = logging.StreamHandler()

            # log levels set
            file_handler_log_level = LOG_LEVELS.get(
                self.native_file_handler_config.get('level'))
            console_handler_log_level = LOG_LEVELS.get(
                self.native_console_handler_config.get('level'))

            file_handler.setLevel(file_handler_log_level)  # type: ignore
            console_handler.setLevel(console_handler_log_level)  # type: ignore

            # formatters
            console_handler.setFormatter(
                logging.Formatter(
                    self.native_console_handler_config.get('format'))
            )

            file_handler.setFormatter(
                logging.Formatter(
                    self.native_file_handler_config.get('format'))
            )
            # handler assignment
            logger.addHandler(console_handler)
            logger.addHandler(file_handler)

        else:
            raise ValueError(f'Invalid logger_identifier.'
                             f'You provided "{logger_identifier}", where we only'
                             f'support either "{LogUtils.NATIVE_LOGGER}"')

        return logger


def tracelog(log: logging.Logger) -> Callable:
    """Decorator to log trace of function entry and exit. Registers a debug log
    for every entry an exit of function through the provided logger.

    :param log: logger object
    :type log: logging.Logger

    :return: Callable
    :rtype: Callable
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            log.debug('entered [{0}] method.'.format(func.__name__))
            result = func(*args, **kwargs)
            log.debug('exited [{0}] method.'.format(func.__name__))
            return result
        return wrapper
    return decorator


# Adapted from logging.handlers.TimedRotatingFileHandler and
# logging.handlers RotatingFileHandler.
# This Class simple overrides necessary functions to check for maxBytes
# and time interval to perform log rotations
# Credit: https://stackoverflow.com/a/6347764
class SizeAndTimeRotatingFileHandler(TimedRotatingFileHandler):
    def __init__(self,
                 filename: str,
                 when: str = 's',
                 interval: int = 1,
                 backupCount: int = 0,
                 encoding: str = None,
                 delay: int = 0,
                 utc: int = 0,
                 maxBytes: int = 0
                 ) -> None:
        """This is just a combination of TimedRotatingFileHandler and
        RotatingFileHandler (adds maxBytes to TimedRotatingFileHandler)

        #noqa: DAR101
        """
        logging.handlers.TimedRotatingFileHandler.__init__(self, filename, when, interval,
                                                           backupCount, encoding, delay, utc)  # type: ignore
        self.maxBytes = maxBytes

    def shouldRollover(self, record: logging.LogRecord) -> int:
        """Determine if rollover should occur.

        Basically, see if the supplied record would
        cause the file to exceed
        the size limit we have.
        we are also comparing times

        :param record: A  LogRecord Object which is
            ready to be emitted.
        :type record: logging.LogRecord

        :return: either 0 or 1,
        :rtype: int
        """
        if self.stream is None:                 # delay was set...
            self.stream = self._open()
        if self.maxBytes > 0:                   # are we rolling over?
            msg = '%s\n' % self.format(record)
            # due to non-posix-compliant Windows feature
            self.stream.seek(0, 2)
            if self.stream.tell() + len(msg) >= self.maxBytes:
                return 1
        t = int(time.time())
        if t >= self.rolloverAt:
            return 1
        return 0

    def doRollover(self):
        """do a rollover; in this case, a date/time stamp is appended to the
        filename when the rollover happens.

        However, you want the file to be named for the start of the
        interval, not the current time.  If there is a backup count,
        then we have to get a list of matching filenames, sort them and
        remove the one with the oldest suffix.
        """
        if self.stream:
            self.stream.close()
        # get the time that this sequence started at and make it a TimeTuple
        currentTime = int(time.time())
        dstNow = time.localtime(currentTime)[-1]
        t = self.rolloverAt - self.interval
        if self.utc:
            timeTuple = time.gmtime(t)
        else:
            timeTuple = time.localtime(t)
            dstThen = timeTuple[-1]
            if dstNow != dstThen:
                if dstNow:
                    addend = 3600
                else:
                    addend = -3600
                timeTuple = time.localtime(t + addend)
        dfn = self.baseFilename + '.' + time.strftime(self.suffix, timeTuple)
        if self.backupCount > 0:
            cnt = 1
            dfn2 = '%s.%03d' % (dfn, cnt)
            while os.path.exists(dfn2):
                dfn2 = '%s.%03d' % (dfn, cnt)
                cnt += 1
            os.rename(self.baseFilename, dfn2)
            for s in self.getFilesToDelete():
                os.remove(s)
        else:
            if os.path.exists(dfn):
                os.remove(dfn)
                os.rename(self.baseFilename, dfn)
        self.mode = 'w'
        self.stream = self._open()
        newRolloverAt = self.computeRollover(currentTime)
        while newRolloverAt <= currentTime:
            newRolloverAt = newRolloverAt + self.interval
        # If DST changes and midnight or weekly rollover, adjust for this.
        if (self.when == 'MIDNIGHT' or self.when.startswith('W')) and not self.utc:
            dstAtRollover = time.localtime(newRolloverAt)[-1]
            if dstNow != dstAtRollover:
                if not dstNow:  # DST kicks in before next rollover, so we need to deduct an hour
                    addend = -3600
                else:           # DST bows out before next rollover, so we need to add an hour
                    addend = 3600
                newRolloverAt += addend
        self.rolloverAt = newRolloverAt

    def getFilesToDelete(self) -> list:
        """Determine the files to delete when rolling over.

        More specific than the earlier method, which just used glob.glob().

        :return: files to delete
        :rtype: list
        """
        dirName, baseName = os.path.split(self.baseFilename)
        fileNames = os.listdir(dirName)
        result = []
        prefix = baseName + '.'
        plen = len(prefix)
        for fileName in fileNames:
            if fileName[:plen] == prefix:
                suffix = fileName[plen:-4]
                if self.extMatch.match(suffix):  # type: ignore
                    result.append(os.path.join(dirName, fileName))
        result.sort()
        if len(result) < self.backupCount:  # type: ignore
            result = []
        else:
            result = result[:len(result) - self.backupCount]  # type: ignore
        return result
