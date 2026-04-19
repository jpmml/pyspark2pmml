from __future__ import annotations

from numbers import Number
from py4j.java_gateway import JavaClass, JavaObject
from pyspark.ml import Transformer
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from pyspark2pmml import shared, spark34, spark35, spark40, spark41
from pyspark2pmml.wrapper import _jvm
from pyspark2pmml.util import load_jars
from types import ModuleType
from typing import List, Optional, Union

import os
import pyspark

from .metadata import __copyright__, __license__, __version__

PMML = JavaObject

def _spark_module(version: str) -> ModuleType:
	if version.startswith("3.4."):
		return spark34
	elif version.startswith("3.5."):
		return spark35
	elif version.startswith("4.0."):
		return spark40
	elif version.startswith("4.1."):
		return spark41
	else:
		raise ValueError("Apache Spark version {version} is not supported".format(version = version))

def _jars(version: str = None) -> List[str]:
	if version is None:
		version = pyspark.__version__
	spark_module = _spark_module(version)
	spark_jars = load_jars(os.path.dirname(spark_module.__file__))
	shared_jars = load_jars(os.path.dirname(shared.__file__))
	return spark_jars + shared_jars

def spark_jars(version: str = None) -> str:
	return ",".join(_jars(version = version))

class PMMLBuilder(object):

	def __init__(self, schema: Union[StructType, DataFrame], pipelineStage: Transformer) -> None:
		jvm = _jvm()
		javaPmmlBuilderClass = jvm.org.jpmml.sparkml.PMMLBuilder
		if not isinstance(javaPmmlBuilderClass, JavaClass):
			raise RuntimeError("JPMML-SparkML not found on classpath")
		if isinstance(schema, StructType):
			javaSchema = jvm.org.apache.spark.sql.types.DataType.fromJson(schema.json())
		elif isinstance(schema, DataFrame):
			javaDf = schema._jdf
			javaSchema = javaDf.schema()
		else:
			raise TypeError("Schema is not a StructType or DataFrame")
		if isinstance(pipelineStage, Transformer):
			javaPipelineStage = pipelineStage._to_java()
		else:
			raise TypeError("Pipeline stage is not a Transformer")
		javaPmmlBuilder = javaPmmlBuilderClass(javaSchema, javaPipelineStage)
		self.javaPmmlBuilder = javaPmmlBuilder

	def build(self) -> PMML:
		return self.javaPmmlBuilder.build()

	def buildByteArray(self) -> bytes:
		return self.javaPmmlBuilder.buildByteArray()

	def buildString(self) -> str:
		return self.javaPmmlBuilder.buildString()

	def buildFile(self, path: str) -> str:
		jvm = _jvm()
		javaFile = jvm.java.io.File(path)
		javaFile = self.javaPmmlBuilder.buildFile(javaFile)
		return javaFile.getAbsolutePath()

	def putOption(self, pipelineStage: Optional[Transformer], key: str, value: Union[str, Number]) -> PMMLBuilder:
		if pipelineStage is None:
			self.javaPmmlBuilder.putOption(key, value)
		else:
			javaPipelineStage = pipelineStage._to_java()
			self.javaPmmlBuilder.putOption(javaPipelineStage, key, value)
		return self

	def putFieldName(self, column: str, name: str) -> PMMLBuilder:
		self.javaPmmlBuilder.putFieldName(column, name)
		return self

	def putFieldNames(self, column: str, names: List[str]) -> PMMLBuilder:
		self.javaPmmlBuilder.putFieldNames(column, names)
		return self

	def verify(self, df: DataFrame, precision: float = 1e-14, zeroThreshold: float = 1e-14) -> PMMLBuilder:
		javaDf = df._jdf
		self.javaPmmlBuilder.verify(javaDf, precision, zeroThreshold)
		return self
