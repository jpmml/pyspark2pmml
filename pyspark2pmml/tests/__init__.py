#!/usr/bin/env python

import findspark

findspark.init()

import os

testDir = os.path.dirname(__file__)
pyspark2pmmlJarFile = os.path.join(os.path.join(testDir, "../.."), "target/pyspark2pmml-1.4-SNAPSHOT.jar")

os.environ['PYSPARK_SUBMIT_ARGS'] = ("--master local[2] --jars " + pyspark2pmmlJarFile + " pyspark-shell")

irisCsvFile = os.path.join(testDir, "resources/Iris.csv")

from pyspark.context import SparkContext
from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.feature import RFormula
from pyspark.sql import SQLContext
from pyspark2pmml import toPMMLBytes
from unittest import TestCase

class PMMLTest(TestCase):

	def setUp(self):
		self.sc = SparkContext()
		self.sqlContext = SQLContext(self.sc)

	def tearDown(self):
		self.sc.stop()

	def testWorkflow(self):
		df = self.sqlContext.read.csv(irisCsvFile, header = True, inferSchema = True)
		
		formula = RFormula(formula = "Species ~ .")
		classifier = DecisionTreeClassifier()
		pipeline = Pipeline(stages = [formula, classifier])
		pipelineModel = pipeline.fit(df)
		
		pmmlBytes = toPMMLBytes(self.sc, df, pipelineModel)
		pmmlString = pmmlBytes.decode("UTF-8")
		self.assertTrue(pmmlString.find("<PMML xmlns=\"http://www.dmg.org/PMML-4_3\" version=\"4.3\">") > -1)
