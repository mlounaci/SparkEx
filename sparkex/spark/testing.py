import os
import unittest

import sparkex.utils as uts
from sparkex.spark.context import Spark


class BaseTestSparkContext(unittest.TestCase):
    params = None
    sc = None
    logger = None

    @classmethod
    def setUpClass(cls):
        # Get the environment variable containing the test parameters file path
        test_param_file = os.getenv("TEST_PARAMS")
        if test_param_file is not None:
            cls.params = uts.getjsonparams(test_param_file)
        spark = Spark.instance()
        cls.sc = spark.get_sc()
        cls.logger = spark.get_logger()
        spark.quiet_py4j()
        cls.setUpTestData()

    @classmethod
    def tearDownClass(cls):
        cls.tearDownTestData()
        print ("Tear Down")

    @classmethod
    def setUpTestData(cls):
        pass

    @classmethod
    def tearDownTestData(cls):
        pass


class BaseTestHiveContext(unittest.TestCase):
    params = None
    sc = None
    hc = None
    logger = None

    @classmethod
    def setUpClass(cls):
        # Get the environment variable containing the test parameters file path
        test_param_file = os.getenv("TEST_PARAMS")
        if test_param_file is not None:
            cls.params = uts.getjsonparams(test_param_file)
        spark = Spark.instance()
        cls.sc = spark.get_sc()
        cls.hc = spark.get_hc()
        cls.logger = spark.get_logger()
        spark.quiet_py4j()
        cls.setUpTestData()

    @classmethod
    def tearDownClass(cls):
        cls.tearDownTestData()
        print ("Tear Down")

    @classmethod
    def setUpTestData(cls):
        pass

    @classmethod
    def tearDownTestData(cls):
        pass


