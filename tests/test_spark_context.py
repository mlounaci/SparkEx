from sparkex.spark.testing import BaseTestSparkContext
from sparkex.spark.context import BaseSparkContext
from sparkex import utils as uts


class Reduce(BaseSparkContext):

    def rdd_reduce(self):
        self.logger.info("I am testing my logger")
        # start by creating a mockup dataset
        l = [(1, 'hello'), (2, 'world'), (3, 'world')]
        # and create a RDD out of it
        rdd = self.sc.parallelize(l)
        # pass it to the transformation you're unit testing
        return rdd.map(lambda x: (x[1], 1)).reduceByKey(lambda a, b: a + b).collect()


class TestSparkContext(BaseTestSparkContext):

    def test_spark_context(self):
        r = Reduce()
        output = r.rdd_reduce()
        self.logger.info("My test Case logger")
        # since it's unit test let's make an assertion
        self.assertEqual(output[0][1], 2)

