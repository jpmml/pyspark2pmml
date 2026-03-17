from py4j.java_gateway import JavaClass
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from pyspark2pmml.wrapper import _jvm

from .metadata import __copyright__, __license__, __version__

class PMMLBuilder(object):

	def __init__(self, schema, pipelineModel):
		jvm = _jvm()
		if isinstance(schema, StructType):
			javaSchema = jvm.org.apache.spark.sql.types.DataType.fromJson(schema.json())
		elif isinstance(schema, DataFrame):
			javaDf = schema._jdf
			javaSchema = javaDf.schema()
		else:
			raise TypeError("Schema is not a StructType or DataFrame")
		javaPipelineModel = pipelineModel._to_java()
		javaPmmlBuilderClass = jvm.org.jpmml.sparkml.PMMLBuilder
		if not isinstance(javaPmmlBuilderClass, JavaClass):
			raise RuntimeError("JPMML-SparkML not found on classpath")
		javaPmmlBuilder = javaPmmlBuilderClass(javaSchema, javaPipelineModel)
		self.javaPmmlBuilder = javaPmmlBuilder

	def build(self):
		return self.javaPmmlBuilder.build()

	def buildByteArray(self):
		return self.javaPmmlBuilder.buildByteArray()

	def buildString(self):
		return self.javaPmmlBuilder.buildString()

	def buildFile(self, path):
		jvm = _jvm()
		javaFile = jvm.java.io.File(path)
		javaFile = self.javaPmmlBuilder.buildFile(javaFile)
		return javaFile.getAbsolutePath()

	def putOption(self, pipelineStage, key, value):
		if pipelineStage is None:
			self.javaPmmlBuilder.putOption(key, value)
		else:
			javaPipelineStage = pipelineStage._to_java()
			self.javaPmmlBuilder.putOption(javaPipelineStage, key, value)
		return self

	def putFieldName(self, column, name):
		self.javaPmmlBuilder.putFieldName(column, name)
		return self

	def putFieldNames(self, column, names):
		self.javaPmmlBuilder.putFieldNames(column, names)
		return self

	def verify(self, df, precision = 1e-14, zeroThreshold = 1e-14):
		javaDf = df._jdf
		self.javaPmmlBuilder.verify(javaDf, precision, zeroThreshold)
		return self
