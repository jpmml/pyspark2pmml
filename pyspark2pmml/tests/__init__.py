#!/usr/bin/env python

import findspark

findspark.init()

import os

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
			.putOption(classifier, "compact", True)
		pmmlBytes = pmmlBuilder.buildByteArray()
		pmmlString = pmmlBytes.decode("UTF-8")
		self.assertTrue(pmmlString.find("<PMML xmlns=\"http://www.dmg.org/PMML-4_3\" version=\"4.3\">") > -1)
