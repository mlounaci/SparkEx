import logging
from sys import getsizeof

from pyspark.context import SparkContext, SparkConf
from pyspark.sql import HiveContext

class Singleton:
    """
    Helper class meant to ease the creation of singletons. This
    should be used as a decorator -- not a metaclass -- to the class
    that should be a singleton.

    The decorated class should define only one `__init__` function
    that takes only the two arguments. Other than that, there are
    no restrictions that apply to the decorated class.

    To get the singleton instance, use the `instance` method. Trying
    to use `__call__` will result in a `SingletonError` being raised.

    """

    _singletons = dict()

    def __init__(self, decorated):
        self._decorated = decorated

    def instance(self):
        """
        Returns the singleton instance. Upon its first call, it creates a
        new instance of the decorated class and calls its `__init__` method.
        On all subsequent calls, the already created instance is returned.

        """
        key = self._decorated.__name__

        try:
            return Singleton._singletons[key]
        except KeyError:
            Singleton._singletons[key] = self._decorated()
            return Singleton._singletons[key]

    def __call__(self):
        """
        Call method that raises an exception in order to prevent creation
        of multiple instances of the singleton. The `instance` method should
        be used instead.

        """
        raise SingletonError(
            'Singletons must be accessed through the `instance` method.')


class SingletonError(Exception):
    pass


@Singleton
class Spark:

    _sc = None
    _hc = None

    def __init__(self):
        conf = SparkConf()
        self._sc = SparkContext(conf=conf)
        self._hc = HiveContext(self._sc)

    def get_sc(self):
        return self._sc

    def get_hc(self):
        return self._hc

    def get_logger(self):
        return (self._sc._jvm.org.apache.log4j.LogManager.getLogger(self._sc.appName)
                or self._create_local_logger())

    def quiet_py4j(self):
        """ turn down spark logging for the test context """
        logger = self._sc._jvm.org.apache.log4j
        logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
        logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

    def _create_local_logger(self):
        logger = logging.getLogger('TestMyApp')
        logger.setLevel(logging.DEBUG)
        # create console handler with a higher log level
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        # create formatter and add it to the handlers
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(name)s: %(message)s',
                                      datefmt="%y/%m/%d %H:%M:%S")
        ch.setFormatter(formatter)
        # add the handlers to the logger
        logger.addHandler(ch)
        return logger


class BaseSparkContext:
    sc = None
    logger = None
    test_mode = False

    def __init__(self, test_mode=False):
        spark = Spark.instance()
        self.sc = spark.get_sc()
        self.logger = spark.get_logger()
        self.test_mode = test_mode

    def get_sc(self):
        return self.sc


class BaseHiveContext:
    hc = None
    sc = None
    logger = None
    test_mode = False

    def __init__(self, test_mode=False):
        spark = Spark.instance()
        self.sc = spark.get_sc()
        self.hc = spark.get_hc()
        self.logger = spark.get_logger()
        self.test_mode = test_mode

    def get_sc(self):
        return self.sc

    def get_hc(self):
        return self.hc
