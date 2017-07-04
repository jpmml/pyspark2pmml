#!/usr/bin/env python

from py4j.java_gateway import JavaClass
from pyspark.mllib.common import _py2java

from .metadata import __copyright__, __license__, __version__

def toPMMLBytes(sc, df, pipelineModel):
	javaDF = _py2java(sc, df)
	javaSchema = javaDF.schema.__call__()
	
	javaStages = sc._jvm.java.util.ArrayList()
	for stage in pipelineModel.stages:
		javaStages.add(stage._java_obj)
	javaPipelineModel = sc._jvm.org.apache.spark.ml.PipelineModel(pipelineModel.uid, javaStages)
	
	javaConverter = sc._jvm.org.jpmml.sparkml.ConverterUtil
	if(not isinstance(javaConverter, JavaClass)):
		raise RuntimeError("JPMML-SparkML not found on classpath")
	return javaConverter.toPMMLByteArray(javaSchema, javaPipelineModel)
