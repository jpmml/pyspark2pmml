#!/usr/bin/env python

from py4j.java_gateway import JavaClass
from pyspark.ml.common import _py2java

from .metadata import __copyright__, __license__, __version__

def toPMMLBytes(sc, df, pipelineModel):
	javaDF = _py2java(sc, df)
	javaSchema = javaDF.schema.__call__()
	
	javaPipelineModel = pipelineModel._to_java()
	
	javaConverter = sc._jvm.org.jpmml.sparkml.ConverterUtil
	if(not isinstance(javaConverter, JavaClass)):
		raise RuntimeError("JPMML-SparkML not found on classpath")
	return javaConverter.toPMMLByteArray(javaSchema, javaPipelineModel)
