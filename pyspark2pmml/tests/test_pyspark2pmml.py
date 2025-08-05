import findspark

findspark.init()

import os
import tempfile

os.environ['PYSPARK_SUBMIT_ARGS'] = ("--master local[2] --jars " + os.environ['JPMML_SPARKML_JAR'] + " pyspark-shell")

from py4j.java_gateway import JavaObject
from pyspark.context import SparkContext
from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.feature import RFormula
from pyspark.sql import SQLContext
from pyspark2pmml import PMMLBuilder
from unittest import TestCase

class PMMLTest(TestCase):

	def setUp(self):
		self.sc = SparkContext()
		self.sqlContext = SQLContext(self.sc)

	def tearDown(self):
		self.sc.stop()

	def testWorkflow(self):
		df = self.sqlContext.read.csv(os.path.join(os.path.dirname(__file__), "resources/Iris.csv"), header = True, inferSchema = True)
		
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
		self.assertTrue("<PMML xmlns=\"http://www.dmg.org/PMML-4_3\" xmlns:data=\"http://jpmml.org/jpmml-model/InlineTable\" version=\"4.3\">" in pmmlString)
		self.assertTrue("<VerificationFields>" in pmmlString)

		pmmlBuilder = pmmlBuilder.putOption(classifier, "compact", False)
		nonCompactFile = tempfile.NamedTemporaryFile(prefix = "pyspark2pmml-", suffix = ".pmml")
		nonCompactPmmlPath = pmmlBuilder.buildFile(nonCompactFile.name)

		pmmlBuilder = pmmlBuilder.putOption(classifier, "compact", True)
		compactFile = tempfile.NamedTemporaryFile(prefix = "pyspark2pmml-", suffix = ".pmml")
		compactPmmlPath = pmmlBuilder.buildFile(compactFile.name)

		self.assertGreater(os.path.getsize(nonCompactPmmlPath), os.path.getsize(compactPmmlPath) + 100)
