import findspark

findspark.init()

from py4j.java_gateway import JavaObject
from pyspark.context import SparkContext
from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.feature import RFormula
from pyspark.sql import SparkSession
from pyspark2pmml import PMMLBuilder
from unittest import TestCase

import os
import tempfile

jpmml_sparkml_packages = os.environ["JPMML_SPARKML_PACKAGES"]

class PMMLTest(TestCase):

	@classmethod
	def setUpClass(cls):
		spark_builder = SparkSession.builder \
			.appName("PMMLTest") \
			.master("local[2]")

		if jpmml_sparkml_packages:
			spark_builder.config("spark.jars.packages", jpmml_sparkml_packages)

		cls.spark = spark_builder.getOrCreate()

		cls.sc = cls.spark.sparkContext

	@classmethod
	def tearDownClass(cls):
		cls.spark.stop()

	def testWorkflow(self):
		df = self.spark.read.csv(os.path.join(os.path.dirname(__file__), "resources/Iris.csv"), header = True, inferSchema = True)
		
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
		
		pmmlString = pmmlByteArray.decode("UTF-8")
		self.assertTrue("<PMML xmlns=\"http://www.dmg.org/PMML-4_4\" xmlns:data=\"http://jpmml.org/jpmml-model/InlineTable\" version=\"4.4\">" in pmmlString)
		self.assertTrue("<VerificationFields>" in pmmlString)

		pmmlBuilder = pmmlBuilder.putOption(classifier, "compact", False)
		nonCompactFile = tempfile.NamedTemporaryFile(prefix = "pyspark2pmml-", suffix = ".pmml")
		nonCompactPmmlPath = pmmlBuilder.buildFile(nonCompactFile.name)

		pmmlBuilder = pmmlBuilder.putOption(classifier, "compact", True)
		compactFile = tempfile.NamedTemporaryFile(prefix = "pyspark2pmml-", suffix = ".pmml")
		compactPmmlPath = pmmlBuilder.buildFile(compactFile.name)

		self.assertGreater(os.path.getsize(nonCompactPmmlPath), os.path.getsize(compactPmmlPath) + 100)
