import findspark

findspark.init()

from pyspark.sql import SparkSession
from unittest import TestCase

import os

JPMML_SPARKML_JARS = os.environ.get("JPMML_SPARKML_JARS", "")
JPMML_SPARKML_PACKAGES = os.environ.get("JPMML_SPARKML_PACKAGES", "")

if JPMML_SPARKML_JARS or JPMML_SPARKML_PACKAGES:
	submit_args = []
	if JPMML_SPARKML_JARS:
		submit_args.append("--jars {}".format(JPMML_SPARKML_JARS))
	if JPMML_SPARKML_PACKAGES:
		submit_args.append("--packages {}".format(JPMML_SPARKML_PACKAGES))
	submit_args.append("pyspark-shell")

	os.environ['PYSPARK_SUBMIT_ARGS'] = " ".join(submit_args)

class PySpark2PMMLTest(TestCase):

	@classmethod
	def setUpClass(cls):
		spark_builder = SparkSession.builder \
			.appName("PMMLTest") \
			.master("local[2]")

		cls.spark = spark_builder.getOrCreate()

		cls.sc = cls.spark.sparkContext

	@classmethod
	def tearDownClass(cls):
		cls.spark.stop()

	def readCsv(self, name):
		csvFile = os.path.join(os.path.dirname(__file__), "resources/{}.csv".format(name))
		return self.spark.read \
			.csv(csvFile, header = True, inferSchema = True)

	def readLibSVM(self, name):
		libsvmFile = os.path.join(os.path.dirname(__file__), "resources/{}.libsvm".format(name))
		return self.spark.read \
			.format("libsvm") \
			.load(libsvmFile)
