import findspark

findspark.init()

from functools import wraps
from py4j.java_gateway import JavaObject
from pyspark.context import SparkContext
from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.feature import RFormula
from pyspark.sql import SparkSession
from pyspark2pmml import PMMLBuilder
from unittest import SkipTest, TestCase

import os
import tempfile

jpmml_sparkml_jars = os.environ.get("JPMML_SPARKML_JARS", "")
jpmml_sparkml_packages = os.environ.get("JPMML_SPARKML_PACKAGES", "")

_pmml_element = "<PMML xmlns=\"http://www.dmg.org/PMML-4_4\" xmlns:data=\"http://jpmml.org/jpmml-model/InlineTable\" version=\"4.4\">"

if jpmml_sparkml_jars or jpmml_sparkml_packages:
	submit_args = []
	if jpmml_sparkml_jars:
		submit_args.append("--jars {}".format(jpmml_sparkml_jars))
	if jpmml_sparkml_packages:
		submit_args.append("--packages {}".format(jpmml_sparkml_packages))
	submit_args.append("pyspark-shell")

	os.environ['PYSPARK_SUBMIT_ARGS'] = " ".join(submit_args)

def requires_pmml_sparkml_xgboost(func):
	@wraps(func)
	def wrapper(*args, **kwargs):
		if "pmml-sparkml-example-executable" not in jpmml_sparkml_jars and "pmml-sparkml-xgboost" not in jpmml_sparkml_packages:
			raise SkipTest()
		return func(*args, **kwargs)
	return wrapper

class PMMLTest(TestCase):

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

	def readDataset(self, name):
		return self.spark.read.csv(os.path.join(os.path.dirname(__file__), "resources/{}.csv".format(name)), header = True, inferSchema = True)

class PySparkTest(PMMLTest):

	def testIris(self):
		df = self.readDataset("Iris")
		
		formula = RFormula(formula = "Species ~ .")
		classifier = DecisionTreeClassifier()
		pipeline = Pipeline(stages = [formula, classifier])
		pipelineModel = pipeline.fit(df)
		
		pmmlBuilder = PMMLBuilder(self.sc, df, pipelineModel) \
			.verify(df.sample(False, 0.1))

		pmml = pmmlBuilder.build()
		self.assertIsInstance(pmml, JavaObject)

		pmmlByteArray = pmmlBuilder.buildByteArray()
		self.assertTrue(isinstance(pmmlByteArray, bytes) or isinstance(pmmlByteArray, bytearray))

		pmmlString = pmmlByteArray.decode("utf-8")
		self.assertTrue(_pmml_element in pmmlString)

		pmmlString = pmmlBuilder.buildString()
		self.assertTrue(_pmml_element in pmmlString)
		self.assertTrue("<VerificationFields>" in pmmlString)

		pmmlBuilder = pmmlBuilder.putOption(classifier, "compact", False)
		with tempfile.NamedTemporaryFile(prefix = "pyspark2pmml-", suffix = ".pmml") as nonCompactFile:
			nonCompactPmmlPath = pmmlBuilder.buildFile(nonCompactFile.name)
			nonCompactSize = os.path.getsize(nonCompactPmmlPath)

		pmmlBuilder = pmmlBuilder.putOption(classifier, "compact", True)
		with tempfile.NamedTemporaryFile(prefix = "pyspark2pmml-", suffix = ".pmml") as compactFile:
			compactPmmlPath = pmmlBuilder.buildFile(compactFile.name)
			compactSize = os.path.getsize(compactPmmlPath)

		self.assertGreater(nonCompactSize, compactSize + 100)

class XGBoostTest(PMMLTest):

	@requires_pmml_sparkml_xgboost
	def testIris(self):
		from pyspark2pmml.xgboost import patch_model
		from xgboost.spark import SparkXGBClassifier

		df = self.readDataset("Iris")

		formula = RFormula(formula = "Species ~ .")
		classifier = SparkXGBClassifier()
		pipeline = Pipeline(stages = [formula, classifier])
		pipelineModel = pipeline.fit(df)

		with self.assertRaises(AttributeError):
			pmmlBuilder = PMMLBuilder(self.sc, df, pipelineModel)

		classifierModel = pipelineModel.stages[-1]

		patch_model(self.sc, classifierModel)

		pmmlBuilder = PMMLBuilder(self.sc, df, pipelineModel)

		pmmlString = pmmlBuilder.buildString()
		self.assertTrue(_pmml_element in pmmlString)

	@requires_pmml_sparkml_xgboost
	def testAuto(self):
		from pyspark2pmml.xgboost import patch_model
		from xgboost.spark import SparkXGBRegressor

		df = self.readDataset("Auto")

		formula = RFormula(formula = "mpg ~ .")
		regressor = SparkXGBRegressor()
		pipeline = Pipeline(stages = [formula, regressor])
		pipelineModel = pipeline.fit(df)

		with self.assertRaises(AttributeError):
			pmmlBuilder = PMMLBuilder(self.sc, df, pipelineModel)

		regressorModel = pipelineModel.stages[-1]

		patch_model(self.sc, regressorModel)

		pmmlBuilder = PMMLBuilder(self.sc, df, pipelineModel)

		pmmlString = pmmlBuilder.buildString()
		self.assertTrue(_pmml_element in pmmlString)
