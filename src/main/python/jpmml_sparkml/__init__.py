#!/usr/bin/env python

from pyspark.ml.common import _py2java

__copyright__ = "Copyright (c) 2016 Villu Ruusmann"
__license__ = "GNU Affero General Public License (AGPL) version 3.0"
__version__ = "${project.python_version}"

def toPMMLBytes(sc, df, pipelineModel):
	javaDF = _py2java(sc, df)
	javaSchema = javaDF.schema.__call__()
	javaPipelineModel = pipelineModel._to_java()
	return sc._jvm.org.jpmml.sparkml.ConverterUtil.toPMMLByteArray(javaSchema, javaPipelineModel)
