from py4j.java_gateway import JavaClass

from .metadata import __copyright__, __license__, __version__

class PMMLBuilder(object):

	def __init__(self, sc, df, pipelineModel):
		javaDf = df._jdf
		javaSchema = javaDf.schema()
		javaPipelineModel = pipelineModel._to_java()
		javaPmmlBuilderClass = sc._jvm.org.jpmml.sparkml.PMMLBuilder
		if(not isinstance(javaPmmlBuilderClass, JavaClass)):
			raise RuntimeError("JPMML-SparkML not found on classpath")
		javaPmmlBuilder = javaPmmlBuilderClass(javaSchema, javaPipelineModel)
		self.sc = sc
		self.javaPmmlBuilder = javaPmmlBuilder

	def build(self):
		return self.javaPmmlBuilder.build()

	def buildByteArray(self):
		return self.javaPmmlBuilder.buildByteArray()

	def buildString(self):
		return self.javaPmmlBuilder.buildString()

	def buildFile(self, path):
		javaFile = self.sc._jvm.java.io.File(path)
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
