from pyspark2pmml.tests import PySpark2PMMLTest

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer
from pyspark.ml.linalg import DenseVector, Vectors, VectorUDT
from pyspark.sql.types import DoubleType, StructType, StructField, StringType
from pyspark2pmml.ml.feature import CategoricalDomain, ContinuousDomain, InvalidCategoryTransformer, VectorDensifier, VectorDisassembler
from tempfile import TemporaryDirectory

import math

def _clone(obj):
	cls = obj.__class__

	with TemporaryDirectory() as tmpDir:
		obj.write() \
			.overwrite() \
			.save(tmpDir)

		cloned_obj = cls.read() \
			.load(tmpDir)

		return cloned_obj

def _escape_df(df):
	def _escape_row(row):
		def _escape_value(value):
			if isinstance(value, float) and math.isnan(value):
				return "NaN"
			return value
		return tuple([_escape_value(element) for element in row])
	return [_escape_row(row) for row in df]

class FeatureTest(PySpark2PMMLTest):

	def _check(self, obj):
		return obj

	def _checked_clone(self, obj):
		self._check(obj)

		obj = _clone(obj)
		self._check(obj)

		return obj

class DomainTest(FeatureTest):
	pass

class CategoricalDomainTest(DomainTest):

	def _check(self, obj):
		self.assertEqual(8 + 1, len(obj.params))

		self.assertEqual(["fruit", "color"], obj.getInputCols())
		self.assertEqual(["pmml_fruit", "pmml_color"], obj.getOutputCols())

		self.assertEqual("asValue", obj.getMissingValueTreatment())
		self.assertEqual("(other)", obj.getMissingValueReplacement())
		self.assertEqual("asMissing", obj.getInvalidValueTreatment())
		self.assertIsNone(obj.getInvalidValueReplacement())

		self.assertTrue(obj.getWithData())

		dataValues = {
			"fruit" : ["apple", "orange"],
			"color" : ["green", "yellow", "red"]
		}

		self.assertEqual(dataValues, obj.getDataValues())

	def test_fit_transform(self):
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

		dataValues = {
			"fruit" : ["apple", "orange"],
			"color" : ["green", "yellow", "red"]
		}

		domain = self._checked_clone(CategoricalDomain() \
			.setInputCols(["fruit", "color"]) \
			.setOutputCols(["pmml_fruit", "pmml_color"]) \
			.setMissingValueTreatment("asValue") \
			.setMissingValueReplacement("(other)") \
			.setInvalidValueTreatment("asMissing")
			.setDataValues(dataValues)
		)

		domain_model = self._checked_clone(domain.fit(df))

		transformed_df = domain_model.transform(df) \
			.select("pmml_fruit", "pmml_color")

		expected_rows = [
			("apple", "red"),
			("apple", "(other)"),
			("orange", "(other)"),
			("(other)", "yellow"),
			("(other)", "green"),
			("apple", "green"),
			("(other)", "(other)")
		]

		self.assertEqual(expected_rows, transformed_df.collect())

class ContinuousDomainTest(DomainTest):

	def _check(self, obj):
		self.assertEqual(8 + 4, len(obj.params))

		self.assertEqual(["width", "height"], obj.getInputCols())
		self.assertEqual(["pmml_width", "pmml_height"], obj.getOutputCols())

		self.assertEqual("asValue", obj.getMissingValueTreatment())
		self.assertEqual(-1, obj.getMissingValueReplacement())
		self.assertEqual("asMissing", obj.getInvalidValueTreatment())
		self.assertIsNone(obj.getInvalidValueReplacement())

		self.assertTrue(obj.getWithData())

		self.assertEqual("asMissingValues", obj.getOutlierTreatment())
		self.assertEqual(20.0, obj.getLowValue())
		self.assertEqual(80.0, obj.getHighValue())

		dataRanges = {
			"width" : [0, 100],
			"height" : [0, 100]
		}

		self.assertEqual(dataRanges, obj.getDataRanges())

	def test_fit_transform(self):
		schema = StructType([
			StructField("width", DoubleType(), True),
			StructField("height", DoubleType(), True)
		])

		rows = [
			(20.0, 10.0),
			(None, 20.0),
			(-999.0, None),
			(10.0, float("NaN")),
			(150.0, 50.0)
		]

		df = self.spark.createDataFrame(rows, schema)

		dataRanges = {
			"width" : [0, 100],
			"height" : [0, 100]
		}

		domain = self._checked_clone(ContinuousDomain() \
			.setInputCols(["width", "height"]) \
			.setOutputCols(["pmml_width", "pmml_height"]) \
			.setMissingValueTreatment("asValue") \
			.setMissingValueReplacement(-1) \
			.setInvalidValueTreatment("asMissing") \
			.setOutlierTreatment("asMissingValues") \
			.setLowValue(20.0) \
			.setHighValue(80.0) \
			.setDataRanges(dataRanges)
		)

		domain_model = self._checked_clone(domain.fit(df))

		transformed_df = domain_model.transform(df) \
			.select("pmml_width", "pmml_height")

		expected_rows = [
			(20.0, -1),
			(-1, 20.0),
			(-1, -1),
			(-1, -1),
			(-1, 50.0)
		]

		self.assertEqual(expected_rows, transformed_df.collect())

class InvalidCategoryTransformerTest(FeatureTest):

	def test_fit_transform(self):
		schema = StructType([
			StructField("fruit", StringType(), True),
			StructField("color", StringType(), True),
			StructField("rating", DoubleType(), False)
		])

		rows = [
			("apple", "red", 2.0),
			("orange", "orange", 3.0),
			("banana", "yellow", 3.0),
			("banana", "green", 1.0),
			("apple", "green", 2.0),
		]

		df = self.spark.createDataFrame(rows, schema)

		indexer = StringIndexer() \
			.setStringOrderType("alphabetAsc") \
			.setInputCols(["fruit", "color", "rating"]) \
			.setOutputCols(["fruitIdx", "colorIdx", "ratingIdx"]) \
			.setHandleInvalid("keep")

		multi_transformer = self._checked_clone(InvalidCategoryTransformer() \
			.setInputCols(["fruitIdx", "colorIdx"]) \
			.setOutputCols(["fruitIdxTransformed", "colorIdxTransformed"])
		)

		single_transformer = self._checked_clone(InvalidCategoryTransformer() \
			.setInputCol("ratingIdx") \
			.setOutputCol("ratingIdxTransformed")
		)

		pipeline = Pipeline(stages = [indexer, multi_transformer, single_transformer])

		pipeline_model = pipeline.fit(df)

		transformed_df = pipeline_model.transform(df)

		def _get_categories(col):
			field = transformed_df.schema[col]
			metadata = field.metadata
			ml_attr = metadata.get("ml_attr", {})
			return ml_attr.get("vals")

		self.assertEqual(["apple", "banana", "orange", "__unknown"], _get_categories("fruitIdx"))
		self.assertEqual(["green", "orange", "red", "yellow", "__unknown"], _get_categories("colorIdx"))
		self.assertEqual(["1.0", "2.0", "3.0", "__unknown"], _get_categories("ratingIdx"))

		self.assertEqual(["apple", "banana", "orange"], _get_categories("fruitIdxTransformed"))
		self.assertEqual(["green", "orange", "red", "yellow"], _get_categories("colorIdxTransformed"))
		self.assertEqual(["1.0", "2.0", "3.0"], _get_categories("ratingIdxTransformed"))

		test_rows = [
			(None, "yellow", 0.0),
			("apple", "", 1.0),
			("banana", "red", float("NaN"))
		]

		test_df = self.spark.createDataFrame(test_rows, schema)

		transformed_test_df = pipeline_model.transform(test_df) \
			.select("fruitIdxTransformed", "colorIdxTransformed", "ratingIdxTransformed")

		expected_test_rows = [
			(float("NaN"), 3.0, float("NaN")),
			(0.0, float("NaN"), 0.0),
			(1.0, 2.0, float("NaN"))
		]

		self.assertEqual(_escape_df(expected_test_rows), _escape_df(transformed_test_df.collect()))

class VectorDensifierTest(FeatureTest):

	def test_fit_transform(self):
		schema = StructType([
			StructField("features", VectorUDT(), True)
		])

		rows = [
			(Vectors.sparse(3, [1], [1.0]), ),
			(Vectors.dense([0.0, 0.0, 1.0]), ),
			(Vectors.sparse(3, [0], [1.0]), )
		]

		df = self.spark.createDataFrame(rows, schema)

		transformer = self._checked_clone(VectorDensifier() \
			.setInputCol("features") \
			.setOutputCol("denseFeatures")
		)

		transformed_df = transformer.transform(df) \
			.select("denseFeatures")

		expected_rows = [
			(Vectors.dense([0.0, 1.0, 0.0]), ),
			(Vectors.dense([0.0, 0.0, 1.0]), ),
			(Vectors.dense([1.0, 0.0, 0.0]), )
		]

		actual_rows = transformed_df.collect()

		self.assertEqual(expected_rows, actual_rows)

		vectors = [actual_row.denseFeatures for actual_row in actual_rows]
		for vector in vectors:
			self.assertIsInstance(vector, DenseVector)

class VectorDisassemblerTest(FeatureTest):

	def test_fit_transform(self):
		schema = StructType([
			StructField("features", VectorUDT(), True)
		])

		rows = [
			(Vectors.sparse(3, [1], [1.0]), ),
			(Vectors.dense(0.0, 0.0, 1.0), ),
			(Vectors.sparse(3, [0], [1.0]), )
		]

		df = self.spark.createDataFrame(rows, schema)

		transformer = self._checked_clone(VectorDisassembler() \
			.setInputCol("features") \
			.setOutputCols(["first", "second", "third"])
		)

		transformed_df = transformer.transform(df) \
			.select("first", "second", "third")

		expected_rows = [
			(None, 1.0, None),
			(0.0, 0.0, 1.0),
			(1.0, None, None)
		]

		actual_rows = transformed_df.collect()

		self.assertEqual(expected_rows, actual_rows)
