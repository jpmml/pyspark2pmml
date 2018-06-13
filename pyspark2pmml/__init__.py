#!/usr/bin/env python

from py4j.java_gateway import JavaObject
from pyspark.ml.common import _py2java

from .metadata import __copyright__, __license__, __version__

class PMMLBuilder(object):

	def __init__(self, sc, df, pipelineModel):
		javaDf = _py2java(sc, df)
		javaSchema = javaDf.schema.__call__()
		javaPipelineModel = pipelineModel._to_java()
		javaPmmlBuilder = sc._jvm.org.jpmml.sparkml.PMMLBuilder(javaSchema, javaPipelineModel)
		if(not isinstance(javaPmmlBuilder, JavaObject)):
			raise RuntimeError("JPMML-SparkML not found on classpath")
		self.sc = sc
		self.javaPmmlBuilder = javaPmmlBuilder

	def build(self):
		return self.javaPmmlBuilder.build()

	def buildByteArray(self):
		return self.javaPmmlBuilder.buildByteArray()

	def putOption(self, pipelineStage, key, value):
		javaPipelineStage = pipelineStage._to_java()
		self.javaPmmlBuilder.putOption(javaPipelineStage, _py2java(self.sc, key), _py2java(self.sc, value))
		return self

def toPMMLBytes(sc, df, pipelineModel):
	pmmlBuilder = PMMLBuilder(sc, df, pipelineModel)
	return pmmlBuilder.buildByteArray()
