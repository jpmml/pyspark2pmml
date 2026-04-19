from pyspark.sql import SparkSession
from pyspark2pmml import classpath
from tempfile import TemporaryDirectory
from unittest import TestCase

import os

def _clone(obj):
	with TemporaryDirectory() as tmpDir:
		obj.write() \
			.overwrite() \
			.save(tmpDir)

		cloned_obj = type(obj) \
			.load(tmpDir)

		return cloned_obj

class PySpark2PMMLTest(TestCase):

	@classmethod
	def setUpClass(cls):
		cls.spark = SparkSession.builder \
			.appName("PMMLTest") \
			.master("local[2]") \
			.config("spark.jars", ",".join(classpath())) \
			.getOrCreate()

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
