from pyspark.ml.param import Param, Params, TypeConverters
from pyspark.ml.util import JavaMLWritable, MLReader
from pyspark.ml.wrapper import JavaEstimator, JavaTransformer
from pyspark.sql import SparkSession

_JVM = None

def _jvm():
	global _JVM
	if _JVM is None:
		spark = SparkSession.getActiveSession()
		if spark is None:
			raise RuntimeError("Apache Spark session not found")
		_JVM = spark._jvm
	return _JVM

def _create_java_object(java_class_name):
	return getattr(_jvm(), java_class_name)()

class HasDomainParams(Params):

	inputCols = Param(Params._dummy(), "inputCols", "", typeConverter = TypeConverters.toListString)
	outputCols = Param(Params._dummy(), "outputCols", "", typeConverter = TypeConverters.toListString)

	missingValues = Param(Params._dummy(), "missingValues", "")
	missingValueTreatment = Param(Params._dummy(), "missingValueTreatment", "", typeConverter = TypeConverters.toString)
	missingValueReplacement = Param(Params._dummy(), "missingValueReplacement", "")
	invalidValueTreatment = Param(Params._dummy(), "invalidValueTreatment", "", typeConverter = TypeConverters.toString)
	invalidValueReplacement = Param(Params._dummy(), "invalidValueReplacement", "")

	withData = Param(Params._dummy(), "withData", "", typeConverter = TypeConverters.toBoolean)

	@property
	def params(self):
		return [
			self.inputCols,
			self.outputCols,
			self.missingValues,
			self.missingValueTreatment,
			self.missingValueReplacement,
			self.invalidValueTreatment,
			self.invalidValueReplacement,
			self.withData
		]

	def getInputCols(self):
		return self.getOrDefault(self.inputCols)

	def setInputCols(self, value):
		return self._set(inputCols = value)

	def getOutputCols(self):
		return self.getOrDefault(self.outputCols)

	def setOutputCols(self, value):
		return self._set(outputCols = value)

	def getMissingValues(self):
		return self.getOrDefault(self.missingValues)

	def setMissingValues(self, value):
		return self._set(missingValues = value)

	def getMissingValueTreatment(self):
		return self.getOrDefault(self.missingValueTreatment)

	def setMissingValueTreatment(self, value):
		return self._set(missingValueTreatment = value)

	def getMissingValueReplacement(self):
		return self.getOrDefault(self.missingValueReplacement)

	def setMissingValueReplacement(self, value):
		return self._set(missingValueReplacement = value)

	def getInvalidValueTreatment(self):
		return self.getOrDefault(self.invalidValueTreatment)

	def setInvalidValueTreatment(self, value):
		return self._set(invalidValueTreatment = value)

	def getInvalidValueReplacement(self):
		return self.getOrDefault(self.invalidValueReplacement)

	def setInvalidValueReplacement(self, value):
		return self._set(invalidValueReplacement = value)

	def getWithData(self):
		return self.getOrDefault(self.withData)

	def setWithData(self, value):
		return self._set(withData = value)

class HasCategoricalDomainParams(HasDomainParams):

	dataValues = Param(Params._dummy(), "dataValues", "")

	@property
	def params(self):
		return super().params + [
			self.dataValues
		]

	def getDataValues(self):
		return self.getOrDefault(self.dataValues)

	def setDataValues(self, value):
		return self._set(dataValues = value)

class HasContinuousDomainParams(HasDomainParams):

	outlierTreatment = Param(Params._dummy(), "outlierTreatment", "", typeConverter = TypeConverters.toString)
	lowValue = Param(Params._dummy(), "lowValue", "")
	highValue = Param(Params._dummy(), "highValue", "")

	dataRanges = Param(Params._dummy(), "dataRanges", "")

	@property
	def params(self):
		return super().params + [
			self.outlierTreatment,
			self.lowValue,
			self.highValue,
			self.dataRanges
		]

	def getOutlierTreatment(self):
		return self.getOrDefault(self.outlierTreatment)

	def setOutlierTreatment(self, value):
		return self._set(outlierTreatment = value)

	def getLowValue(self):
		return self.getOrDefault(self.lowValue)

	def setLowValue(self, value):
		return self._set(lowValue = value)

	def getHighValue(self):
		return self.getOrDefault(self.highValue)

	def setHighValue(self, value):
		return self._set(highValue = value)

	def getDataRanges(self):
		return self.getOrDefault(self.dataRanges)

	def setDataRanges(self, value):
		return self._set(dataRanges = value)

class Domain(JavaEstimator["DomainModel"], JavaMLWritable):

	_java_class_name = "org.jpmml.sparkml.feature.Domain"

	def __init__(self, *, java_obj, inputCols = None, outputCols = None, missingValues = None, missingValueTreatment = "asIs", missingValueReplacement = None, invalidValueTreatment = "returnInvalid", invalidValueReplacement = None, withData = True):
		super().__init__(java_obj = java_obj)

		if inputCols is not None:
			self.setInputCols(inputCols)
		if outputCols is not None:
			self.setOutputCols(outputCols)

		if missingValues is not None:
			self.setMissingValues(missingValues)

		if missingValueTreatment != "asIs":
			self.setMissingValueTreatment(missingValueTreatment)
		if missingValueReplacement is not None:
			self.setMissingValueReplacement(missingValueReplacement)

		if invalidValueTreatment != "returnInvalid":
			self.setInvalidValueTreatment(invalidValueTreatment)
		if invalidValueReplacement is not None:
			self.setInvalidValueReplacement(invalidValueReplacement)

		if withData != True:
			self.setWithData(withData)

class DomainModel(JavaTransformer, JavaMLWritable):

	_java_class_name = "org.jpmml.sparkml.feature.DomainModel"

	def __init__(self, java_obj):
		super().__init__(java_obj = java_obj)

class CategoricalDomain(Domain, HasCategoricalDomainParams):

	_java_class_name = "org.jpmml.sparkml.feature.CategoricalDomain"

	def __init__(self, *, java_obj = None, inputCols = None, outputCols = None, missingValues = None, missingValueTreatment = "asIs", missingValueReplacement = None, invalidValueTreatment = "returnInvalid", invalidValueReplacement = None, withData = True, dataValues = None):
		if java_obj is None:
			java_obj = _create_java_object(CategoricalDomain._java_class_name)

		super().__init__(java_obj = java_obj, inputCols = inputCols, outputCols = outputCols, missingValues = missingValues, missingValueTreatment = missingValueTreatment, missingValueReplacement = missingValueReplacement, invalidValueTreatment = invalidValueTreatment, invalidValueReplacement = invalidValueReplacement, withData = withData)

		if dataValues is not None:
			self.setDataValues(dataValues)

	def _create_model(self, java_obj):
		return CategoricalDomainModel(java_obj)

	@classmethod
	def read(cls):
		return _JavaReader(cls, CategoricalDomain._java_class_name)

class CategoricalDomainModel(DomainModel, HasCategoricalDomainParams):

	_java_class_name = "org.jpmml.sparkml.feature.CategoricalDomainModel"

	def __init__(self, java_obj):
		super().__init__(java_obj)

	@classmethod
	def read(cls):
		return _JavaReader(cls, CategoricalDomainModel._java_class_name)

class ContinuousDomain(Domain, HasContinuousDomainParams):

	_java_class_name = "org.jpmml.sparkml.feature.ContinuousDomain"

	def __init__(self, *, java_obj = None, inputCols = None, outputCols = None, missingValues = None, missingValueTreatment = "asIs", missingValueReplacement = None, invalidValueTreatment = "returnInvalid", invalidValueReplacement = None, withData = True, outlierTreatment = "asIs", lowValue = None, highValue = None, dataRanges = None):

		if java_obj is None:
			java_obj = _create_java_object(ContinuousDomain._java_class_name)

		super().__init__(java_obj = java_obj, inputCols = inputCols, outputCols = outputCols, missingValues = missingValues, missingValueTreatment = missingValueTreatment, missingValueReplacement = missingValueReplacement, invalidValueTreatment = invalidValueTreatment, invalidValueReplacement = invalidValueReplacement, withData = withData)

		if outlierTreatment != "asIs":
			self.setOutlierTreatment(outlierTreatment)
		if lowValue is not None:
			self.setLowValue(lowValue)
		if highValue is not None:
			self.setHighValue(highValue)

		if dataRanges is not None:
			self.setDataRanges(dataRanges)

	def _create_model(self, java_obj):
		return ContinuousDomainModel(java_obj)

	@classmethod
	def read(cls):
		return _JavaReader(cls, ContinuousDomain._java_class_name)

class ContinuousDomainModel(DomainModel, HasContinuousDomainParams):

	_java_class_name = "org.jpmml.sparkml.feature.ContinuousDomainModel"

	def __init__(self, java_obj):
		super().__init__(java_obj)

	@classmethod
	def read(cls):
		return _JavaReader(cls, ContinuousDomainModel._java_class_name)

class _JavaReader(MLReader):

	def __init__(self, py_class, java_class_name):
		super().__init__()
		self.py_class = py_class
		self.java_class_name = java_class_name

	def load(self, path):
		java_obj = getattr(_jvm(), self.java_class_name).load(path)
		
		py_obj = self.py_class(java_obj = java_obj)
		py_obj._transfer_params_from_java()
		py_obj._resetUid(java_obj.uid())

		return py_obj
