#!/usr/bin/env python

from py4j.java_gateway import JavaObject
from pyspark.ml.common import _py2java

from .metadata import __copyright__, __license__, __version__

def toPMMLBytes(sc, df, pipelineModel):
	javaDF = _py2java(sc, df)
	javaSchema = javaDF.schema.__call__()
	
	javaPipelineModel = pipelineModel._to_java()
	
	javaPmmlBuilder = sc._jvm.org.jpmml.sparkml.PMMLBuilder(javaSchema, javaPipelineModel)
	if(not isinstance(javaPmmlBuilder, JavaObject)):
		raise RuntimeError("JPMML-SparkML not found on classpath")
	return javaPmmlBuilder.buildByteArray()
