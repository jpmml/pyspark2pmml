#!/usr/bin/env python

import findspark

findspark.init()

import os
import tempfile

os.environ['PYSPARK_SUBMIT_ARGS'] = ("--master local[2] --jars " + os.environ['JPMML_SPARKML_JAR'] + " pyspark-shell")

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
			.verify(df.sample(True, 0.1))

		pmmlBytes = pmmlBuilder.buildByteArray()
		pmmlString = pmmlBytes.decode("UTF-8")

		self.assertTrue(pmmlString.find("<PMML xmlns=\"http://www.dmg.org/PMML-4_3\" xmlns:data=\"http://jpmml.org/jpmml-model/InlineTable\" version=\"4.3\">") > -1)
		self.assertTrue(pmmlString.find("<VerificationFields>") > -1)

		pmmlBuilder.putOption(classifier, "compact", False)
		dtcFile = tempfile.NamedTemporaryFile(prefix = "dtc-", suffix = ".pmml")
		dtcPmmlPath = pmmlBuilder.buildFile(dtcFile.name)

		pmmlBuilder.putOption(classifier, "compact", True)
		dtcCompactFile = tempfile.NamedTemporaryFile(prefix = "dtc-compact-", suffix = ".pmml")
		dtcCompactPmmlPath = pmmlBuilder.buildFile(dtcCompactFile.name)

		self.assertGreater(os.path.getsize(dtcPmmlPath), os.path.getsize(dtcCompactPmmlPath))
