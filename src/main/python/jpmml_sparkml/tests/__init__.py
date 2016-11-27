#!/usr/bin/env python

import findspark

findspark.init()

import os

projectDir = os.path.join(os.path.dirname(__file__), "../../../../..")

irisCsvFile = os.path.join(projectDir, "src/test/resources/Iris.csv")
jpmmlPackageJarFile = os.path.join(projectDir, "target/jpmml-sparkml-package-1.0-SNAPSHOT.jar")

os.environ['PYSPARK_SUBMIT_ARGS'] = ("--master local[2] --jars " + jpmmlPackageJarFile + " pyspark-shell")

from jpmml_sparkml import toPMMLBytes
from pyspark.context import SparkContext
from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.feature import RFormula
from pyspark.sql import SQLContext
from unittest import TestCase

class JPMMLTest(TestCase):

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
