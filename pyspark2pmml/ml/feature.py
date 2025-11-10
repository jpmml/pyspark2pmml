from pyspark.ml.param import Param, Params, TypeConverters
from pyspark.ml.param.shared import HasInputCol, HasInputCols, HasOutputCol, HasOutputCols
from pyspark.ml.util import JavaMLWritable, MLReader
from pyspark.ml.wrapper import JavaEstimator, JavaTransformer
from pyspark.sql import SparkSession
from py4j.java_gateway import JavaObject

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

def _to_objectarray(py_values):
	jvm = _jvm()
	return jvm.java.util.ArrayList(list(py_values)).toArray()

def _from_objectarray(java_values):
	return list(java_values)

def _to_objectarray_map(py_map):
	jvm = _jvm()
	java_map = jvm.org.jpmml.sparkml.feature.DomainUtil.toObjectArrayMap(py_map)
	scala_map = jvm.org.jpmml.sparkml.feature.DomainUtil.toScalaMap(java_map)
	return scala_map

def _to_numberarray_map(py_map):
	jvm = _jvm()
	java_map = jvm.org.jpmml.sparkml.feature.DomainUtil.toNumberArrayMap(py_map)
	scala_map = jvm.org.jpmml.sparkml.feature.DomainUtil.toScalaMap(java_map)
	return scala_map

def _from_array_map(scala_map):
	jvm = _jvm()
	java_map = jvm.org.jpmml.sparkml.feature.DomainUtil.toJavaMap(scala_map)
	py_map = {k : list(v) for k, v in jvm.org.jpmml.sparkml.feature.DomainUtil.toListMap(java_map).items()}
	return py_map

def _from_objectarray_map(scala_map):
	return _from_array_map(scala_map)

def _from_numberarray_map(scala_map):
	return _from_array_map(scala_map)

class HasDomainParams(Params):

	_param_formatters = {
		"missingValues" : _to_objectarray,
		"dataRanges" : _to_numberarray_map,
		"dataValues" : _to_objectarray_map
	}
	_param_parsers = {
		"missingValues" : _from_objectarray,
		"dataRanges" : _from_numberarray_map,
		"dataValues" : _from_objectarray_map
	}

	def __init__(self):
		super().__init__()

		self.inputCols = Param(self, "inputCols", "", typeConverter = TypeConverters.toListString)
		self.outputCols = Param(self, "outputCols", "", typeConverter = TypeConverters.toListString)

		self.missingValues = Param(self, "missingValues", "")
		self.missingValueTreatment = Param(self, "missingValueTreatment", "", typeConverter = TypeConverters.toString)
		self.missingValueReplacement = Param(self, "missingValueReplacement", "")
		self.invalidValueTreatment = Param(self, "invalidValueTreatment", "", typeConverter = TypeConverters.toString)
		self.invalidValueReplacement = Param(self, "invalidValueReplacement", "")

		self._setDefault(
			missingValues = [],
			missingValueTreatment = "asIs",
			missingValueReplacement = None,
			invalidValueTreatment = "returnInvalid",
			invalidValueReplacement = None
		)

		self.withData = Param(self, "withData", "", typeConverter = TypeConverters.toBoolean)

		self._setDefault(withData = True)

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

	def __init__(self):
		super().__init__()

		self.dataValues = Param(self, "dataValues", "")

	def getDataValues(self):
		return self.getOrDefault(self.dataValues)

	def setDataValues(self, value):
		return self._set(dataValues = value)

class HasContinuousDomainParams(HasDomainParams):

	def __init__(self):
		super().__init__()

		self.outlierTreatment = Param(self, "outlierTreatment", "", typeConverter = TypeConverters.toString)
		self.lowValue = Param(self, "lowValue", "")
		self.highValue = Param(self, "highValue", "")

		self._setDefault(
			outlierTreatment = "asIs",
			lowValue = None,
			highValue = None
		)

		self.dataRanges = Param(self, "dataRanges", "")

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

class DomainParamsMixin:

	def _make_java_param_pair(self, param, value):
		param_formatter = HasDomainParams._param_formatters.get(param.name, None)
		if param_formatter is not None and value is not None:
			java_param = self._java_obj.getParam(param.name)
			return java_param.w(param_formatter(value))
		return super()._make_java_param_pair(param, value)

	def _transfer_params_to_java(self):
		super()._transfer_params_to_java()

	def _transfer_params_from_java(self):
		super()._transfer_params_from_java()
		for param in self.params:
			param_parser = HasDomainParams._param_parsers.get(param.name, None)
			if param_parser is None:
				continue
			if param in self._paramMap and self._paramMap[param] is not None:
				self._paramMap[param] = param_parser(self._paramMap[param])
			if param in self._defaultParamMap and self._defaultParamMap[param] is not None:
				self._defaultParamMap[param] = param_parser(self._defaultParamMap[param])

class Domain(DomainParamsMixin, JavaEstimator, JavaMLWritable):

	_java_class_name = "org.jpmml.sparkml.feature.Domain"

class DomainModel(DomainParamsMixin, JavaTransformer, JavaMLWritable):

	_java_class_name = "org.jpmml.sparkml.feature.DomainModel"

class CategoricalDomain(Domain, HasCategoricalDomainParams):

	_java_class_name = "org.jpmml.sparkml.feature.CategoricalDomain"

	def __init__(self, *, java_obj = None, **kwargs):
		if java_obj is None:
			java_obj = _create_java_object(CategoricalDomain._java_class_name)
		super().__init__(java_obj = java_obj)
		self._set(**kwargs)

	def _create_model(self, java_obj):
		return CategoricalDomainModel(java_obj)

	@classmethod
	def read(cls):
		return _JavaReader(cls, CategoricalDomain._java_class_name)

class CategoricalDomainModel(DomainModel, HasCategoricalDomainParams):

	_java_class_name = "org.jpmml.sparkml.feature.CategoricalDomainModel"

	@classmethod
	def read(cls):
		return _JavaReader(cls, CategoricalDomainModel._java_class_name)

class ContinuousDomain(Domain, HasContinuousDomainParams):

	_java_class_name = "org.jpmml.sparkml.feature.ContinuousDomain"

	def __init__(self, *, java_obj = None, **kwargs):
		if java_obj is None:
			java_obj = _create_java_object(ContinuousDomain._java_class_name)
		super().__init__(java_obj = java_obj)
		self._set(**kwargs)

	def _create_model(self, java_obj):
		return ContinuousDomainModel(java_obj)

	@classmethod
	def read(cls):
		return _JavaReader(cls, ContinuousDomain._java_class_name)

class ContinuousDomainModel(DomainModel, HasContinuousDomainParams):

	_java_class_name = "org.jpmml.sparkml.feature.ContinuousDomainModel"

	@classmethod
	def read(cls):
		return _JavaReader(cls, ContinuousDomainModel._java_class_name)

class InvalidCategoryTransformer(JavaTransformer, HasInputCol, HasInputCols, HasOutputCol, HasOutputCols, JavaMLWritable):

	_java_class_name = "org.jpmml.sparkml.feature.InvalidCategoryTransformer"

	def __init__(self, *, java_obj = None, **kwargs):
		if java_obj is None:
			java_obj = _create_java_object(InvalidCategoryTransformer._java_class_name)
		super().__init__(java_obj = java_obj)
		self._set(**kwargs)

	def setInputCol(self, value):
		return self._set(inputCol = value)

	def setInputCols(self, value):
		return self._set(inputCols = value)

	def setOutputCol(self, value):
		return self._set(outputCol = value)

	def setOutputCols(self, value):
		return self._set(outputCols = value)

	@classmethod
	def read(cls):
		return _JavaReader(cls, InvalidCategoryTransformer._java_class_name)

class SparseToDenseTransformer(JavaTransformer, HasInputCol, HasOutputCol, JavaMLWritable):

	_java_class_name = "org.jpmml.sparkml.feature.SparseToDenseTransformer"

	def __init__(self, *, java_obj = None, **kwargs):
		if java_obj is None:
			java_obj = _create_java_object(SparseToDenseTransformer._java_class_name)
		super().__init__(java_obj = java_obj)
		self._set(**kwargs)

	def setInputCol(self, value):
		return self._set(inputCol = value)

	def setOutputCol(self, value):
		return self._set(outputCol = value)

	@classmethod
	def read(cls):
		return _JavaReader(cls, SparseToDenseTransformer._java_class_name)

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
