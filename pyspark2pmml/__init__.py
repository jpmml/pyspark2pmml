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

	def buildFile(self, path):
		javaFile = self.sc._jvm.java.io.File(path)
		javaFile = self.javaPmmlBuilder.buildFile(javaFile)
		return javaFile.getAbsolutePath()

	def putOption(self, pipelineStage, key, value):
		javaKey = _py2java(self.sc, key)
		javaValue = _py2java(self.sc, value)
		if pipelineStage is None:
			self.javaPmmlBuilder.putOption(javaKey, javaValue)
		else:
			javaPipelineStage = pipelineStage._to_java()
			self.javaPmmlBuilder.putOption(javaPipelineStage, javaKey, javaValue)
		return self

	def verify(self, df, precision = 1e-14, zeroThreshold = 1e-14):
		javaDf = _py2java(self.sc, df)
		self.javaPmmlBuilder.verify(javaDf, precision, zeroThreshold)
		return self

def toPMMLBytes(sc, df, pipelineModel):
	raise RuntimeError("Replace \"toPMMLBytes(sc, df, pipelineModel)\" with \"PMMLBuilder(sc, df, pipelineModel).buildByteArray()\"")
