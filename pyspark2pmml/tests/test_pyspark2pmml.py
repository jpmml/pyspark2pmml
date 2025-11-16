from pyspark2pmml.tests import JPMML_SPARKML_JARS, JPMML_SPARKML_PACKAGES, PySpark2PMMLTest

from functools import wraps
from py4j.java_gateway import JavaObject
from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.feature import RFormula
from pyspark2pmml import PMMLBuilder
from unittest import SkipTest

import os
import tempfile

_pmml_element = "<PMML xmlns=\"http://www.dmg.org/PMML-4_4\" xmlns:data=\"http://jpmml.org/jpmml-model/InlineTable\" version=\"4.4\">"

def requires_pmml_sparkml_xgboost(func):
	@wraps(func)
	def wrapper(*args, **kwargs):
		if "pmml-sparkml-example-executable" not in JPMML_SPARKML_JARS and "pmml-sparkml-xgboost" not in JPMML_SPARKML_PACKAGES:
			raise SkipTest()
		return func(*args, **kwargs)
	return wrapper

class PySparkTest(PySpark2PMMLTest):

	def testIrisCsv(self):
		df = self.readCsv("Iris")
		
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

		pmmlBuilder = PMMLBuilder(self.sc, df, pipelineModel)

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

		df = self.readCsv("Auto")

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
