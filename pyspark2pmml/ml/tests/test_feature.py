from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StructType, StructField, StringType
from pyspark2pmml.ml.feature import CategoricalDomain, ContinuousDomain
from tempfile import TemporaryDirectory
from unittest import skipIf, TestCase

import os
import pyspark

_SPARK_VERSION = tuple(map(int, pyspark.__version__.split(".")))

def _require_spark_version(major, minor, patch = 0):
	return _SPARK_VERSION >= (major, minor, patch)

skip_if_legacy = skipIf(not _require_spark_version(3, 4), "Legacy Apache Spark version")

jpmml_sparkml_packages = os.environ["JPMML_SPARKML_PACKAGES"]

def _clone(obj):
	cls = obj.__class__

	with TemporaryDirectory() as tmpDir:
		obj.write() \
			.overwrite() \
			.save(tmpDir)

		cloned_obj = cls.read() \
			.load(tmpDir)

		return cloned_obj

@skip_if_legacy
class DomainTest(TestCase):

	def _check(self, obj):
		return obj

	def _checked_clone(self, obj):
		self._check(obj)

		obj = _clone(obj)
		self._check(obj)

		return obj

	@classmethod
	def setUpClass(cls):
		spark_builder = SparkSession.builder \
			.appName("DomainTest") \
			.master("local[2]")

		if jpmml_sparkml_packages:
			spark_builder.config("spark.jars.packages", jpmml_sparkml_packages)

		cls.spark = spark_builder.getOrCreate()

		cls.sc = cls.spark.sparkContext

	@classmethod
	def tearDownClass(cls):
		cls.spark.stop()

class CategoricalDomainTest(DomainTest):

	def _check(self, obj):
		self.assertEqual(8 + 1, len(obj.params))

		self.assertEqual(["fruit", "color"], obj.getInputCols())
		self.assertEqual(["pmml_fruit", "pmml_color"], obj.getOutputCols())

		self.assertEqual("asIs", obj.getMissingValueTreatment())
		self.assertIsNone(obj.getMissingValueReplacement())
		self.assertEqual("returnInvalid", obj.getInvalidValueTreatment())
		self.assertIsNone(obj.getInvalidValueReplacement())

		self.assertTrue(obj.getWithData())

	def test_fit_transform(self):
		domain = self._checked_clone(CategoricalDomain() \
			.setInputCols(["fruit", "color"]) \
			.setOutputCols(["pmml_fruit", "pmml_color"])
		)

		schema = StructType([
			StructField("fruit", StringType(), True),
			StructField("color", StringType(), True)
		])

		rows = [
			("apple", "red"),
			("apple", None),
			("orange", "orange"),
			("banana", "yellow"),
			("banana", "green"),
			("apple", "green"),
			(None, "pink")
		]

		df = self.spark.createDataFrame(rows, schema)

		domain_model = self._checked_clone(domain.fit(df))

		transformed_df = domain_model.transform(df) \
			.select("pmml_fruit", "pmml_color")

		self.assertEqual(rows, transformed_df.collect())

class ContinuousDomainTest(DomainTest):

	def _check(self, obj):
		self.assertEqual(8 + 4, len(obj.params))

		self.assertEqual(["width", "height"], obj.getInputCols())
		self.assertEqual(["pmml_width", "pmml_height"], obj.getOutputCols())

		self.assertEqual("asIs", obj.getMissingValueTreatment())
		self.assertIsNone(obj.getMissingValueReplacement())
		self.assertEqual("returnInvalid", obj.getInvalidValueTreatment())
		self.assertIsNone(obj.getInvalidValueReplacement())

		self.assertTrue(obj.getWithData())

		self.assertEqual("asIs", obj.getOutlierTreatment())
		self.assertIsNone(obj.getLowValue())
		self.assertIsNone(obj.getHighValue())

	def test_fit_transform(self):
		domain = self._checked_clone(ContinuousDomain() \
			.setInputCols(["width", "height"]) \
			.setOutputCols(["pmml_width", "pmml_height"])
		)

		schema = StructType([
			StructField("width", DoubleType(), True),
			StructField("height", DoubleType(), True)
		])

		rows = [
			(20.0, 10.0),
			(None, 20.0),
			(-999.0, None),
			# XXX
			#(10.0, float("NaN")),
			(150.0, 50.0)
		]

		df = self.spark.createDataFrame(rows, schema)

		domain_model = self._checked_clone(domain.fit(df))

		transformed_df = domain_model.transform(df) \
			.select("pmml_width", "pmml_height")

		self.assertEqual(rows, transformed_df.collect())
