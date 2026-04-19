from functools import wraps
from py4j.java_gateway import JavaObject
from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.feature import RFormula
from pyspark.sql import SparkSession
from pyspark2pmml import classpath, PMMLBuilder
from pyspark2pmml.tests import PySpark2PMMLTest
from unittest import SkipTest, TestCase

import os
import tempfile

_pmml_element = "<PMML xmlns=\"http://www.dmg.org/PMML-4_4\" xmlns:data=\"http://jpmml.org/jpmml-model/InlineTable\" version=\"4.4\">"

def requires_pmml_sparkml_xgboost(func):
	@wraps(func)
	def wrapper(*args, **kwargs):
		spark = SparkSession.getActiveSession()

		jars = spark.conf.get("spark.jars", "")
		if "xgboost4j-spark_2." not in jars:
			raise SkipTest()
		return func(*args, **kwargs)
	return wrapper

class ClasspathTest(TestCase):

	def testClasspath(self):
		spark34_jars = classpath("3.4.")
		spark35_jars = classpath("3.5.")
		spark40_jars = classpath("4.0.")
		spark41_jars = classpath("4.1.")

		self.assertEqual(3 + 15, len(spark34_jars))
		self.assertEqual(3 + 15, len(spark35_jars))
		self.assertEqual(3 + 15, len(spark40_jars))
		self.assertEqual(3 + 15, len(spark41_jars))
		self.assertNotEqual(set(spark34_jars[0:3]), set(spark41_jars[0:3]))
		self.assertEqual(set(spark34_jars[3:]), set(spark41_jars[3:]))

class PySparkTest(PySpark2PMMLTest):

	def testIrisCsv(self):
		df = self.readCsv("Iris")
		
		formula = RFormula(formula = "Species ~ .")
		classifier = DecisionTreeClassifier()
		pipeline = Pipeline(stages = [formula, classifier])
		pipelineModel = pipeline.fit(df)
		
		pmmlBuilder = PMMLBuilder(df.schema, pipelineModel) \
			.verify(df.sample(False, 0.1))

		pmml = pmmlBuilder.build()
		self.assertIsInstance(pmml, JavaObject)

		pmmlByteArray = pmmlBuilder.buildByteArray()
		self.assertTrue(isinstance(pmmlByteArray, bytes) or isinstance(pmmlByteArray, bytearray))

		pmmlString = pmmlByteArray.decode("utf-8")
		self.assertTrue(_pmml_element in pmmlString)

		pmmlString = pmmlBuilder.buildString()
		self.assertTrue(_pmml_element in pmmlString)
		self.assertTrue("Sepal_Length" in pmmlString)
		self.assertTrue("Petal_Length" in pmmlString)
		self.assertTrue("<VerificationFields>" in pmmlString)

		pmmlBuilder = pmmlBuilder \
			.putOption(classifier, "compact", False)

		with tempfile.NamedTemporaryFile(prefix = "pyspark2pmml-", suffix = ".pmml") as nonCompactFile:
			nonCompactPmmlPath = pmmlBuilder.buildFile(nonCompactFile.name)
			nonCompactSize = os.path.getsize(nonCompactPmmlPath)

		pmmlBuilder = pmmlBuilder \
			.putOption(classifier, "compact", True)

		with tempfile.NamedTemporaryFile(prefix = "pyspark2pmml-", suffix = ".pmml") as compactFile:
			compactPmmlPath = pmmlBuilder.buildFile(compactFile.name)
			compactSize = os.path.getsize(compactPmmlPath)

		self.assertGreater(nonCompactSize, compactSize + 100)

	def testIrisLibSVM(self):
		df = self.readLibSVM("Iris")

		classifier = DecisionTreeClassifier()
		pipeline = Pipeline(stages = [classifier])
		pipelineModel = pipeline.fit(df)

		pmmlBuilder = PMMLBuilder(df, pipelineModel)

		pmmlString = pmmlBuilder.buildString()
		self.assertTrue(_pmml_element in pmmlString)
		self.assertFalse("Sepal_Length" in pmmlString)
		self.assertFalse("Petal_Length" in pmmlString)
		self.assertTrue("features[0]" in pmmlString)
		self.assertTrue("features[2]" in pmmlString)

		pmmlBuilder = pmmlBuilder \
			.putFieldNames("features", ["Sepal_Length", "Sepal_Width", "Petal_Length", "Petal_Width"])

		pmmlString = pmmlBuilder.buildString()
		self.assertTrue(_pmml_element in pmmlString)
		self.assertTrue("Sepal_Length" in pmmlString)
		self.assertTrue("Petal_Length" in pmmlString)
		self.assertFalse("features[0]" in pmmlString)
		self.assertFalse("features[2]" in pmmlString)

class XGBoostTest(PySpark2PMMLTest):

	@requires_pmml_sparkml_xgboost
	def testIris(self):
		from pyspark2pmml.xgboost import patch_model
		from xgboost.spark import SparkXGBClassifier

		df = self.readCsv("Iris")

		formula = RFormula(formula = "Species ~ .")
		classifier = SparkXGBClassifier()
		pipeline = Pipeline(stages = [formula, classifier])
		pipelineModel = pipeline.fit(df)

		with self.assertRaises(AttributeError):
			pmmlBuilder = PMMLBuilder(df, pipelineModel)

		classifierModel = pipelineModel.stages[-1]

		patch_model(classifierModel)

		pmmlBuilder = PMMLBuilder(df.schema, pipelineModel)

		pmmlString = pmmlBuilder.buildString()
		self.assertTrue(_pmml_element in pmmlString)

	@requires_pmml_sparkml_xgboost
	def testAuto(self):
		from pyspark2pmml.xgboost import patch_model
		from xgboost.spark import SparkXGBRegressor

		df = self.readCsv("Auto")

		formula = RFormula(formula = "mpg ~ .")
		regressor = SparkXGBRegressor()
		pipeline = Pipeline(stages = [formula, regressor])
		pipelineModel = pipeline.fit(df)

		with self.assertRaises(AttributeError):
			pmmlBuilder = PMMLBuilder(df, pipelineModel)

		regressorModel = pipelineModel.stages[-1]

		patch_model(regressorModel)

		pmmlBuilder = PMMLBuilder(df, pipelineModel)

		pmmlString = pmmlBuilder.buildString()
		self.assertTrue(_pmml_element in pmmlString)
