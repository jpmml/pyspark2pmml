from py4j.java_gateway import JavaObject
from pyspark.ml.param import Param, Params, TypeConverters
from pyspark.ml.param.shared import HasInputCol, HasInputCols, HasOutputCol, HasOutputCols
from pyspark.ml.util import JavaMLWritable
from pyspark.ml.wrapper import JavaEstimator, JavaTransformer
from pyspark2pmml.wrapper import _create_java_object, _from_numberarray_map, _from_objectarray, _from_objectarray_map, _register_jpmml_class, _to_numberarray_map, _to_objectarray, _to_objectarray_map, JPMMLReadable

import warnings

class HasDomainParams(Params):

	inputCols = Param(Params._dummy(), "inputCols", "", typeConverter = TypeConverters.toListString)
	outputCols = Param(Params._dummy(), "outputCols", "", typeConverter = TypeConverters.toListString)

	missingValues = Param(Params._dummy(), "missingValues", "")
	missingValueTreatment = Param(Params._dummy(), "missingValueTreatment", "", typeConverter = TypeConverters.toString)
	missingValueReplacement = Param(Params._dummy(), "missingValueReplacement", "")
	invalidValueTreatment = Param(Params._dummy(), "invalidValueTreatment", "", typeConverter = TypeConverters.toString)
	invalidValueReplacement = Param(Params._dummy(), "invalidValueReplacement", "")

	withData = Param(Params._dummy(), "withData", "", typeConverter = TypeConverters.toBoolean)

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

		self._setDefault(
			missingValues = [],
			missingValueTreatment = "asIs",
			missingValueReplacement = None,
			invalidValueTreatment = "returnInvalid",
			invalidValueReplacement = None,
			withData = True
		)

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

	def __init__(self):
		super().__init__()

	def getDataValues(self):
		return self.getOrDefault(self.dataValues)

	def setDataValues(self, value):
		return self._set(dataValues = value)

class HasContinuousDomainParams(HasDomainParams):

	outlierTreatment = Param(Params._dummy(), "outlierTreatment", "", typeConverter = TypeConverters.toString)
	lowValue = Param(Params._dummy(), "lowValue", "")
	highValue = Param(Params._dummy(), "highValue", "")

	dataRanges = Param(Params._dummy(), "dataRanges", "")

	def __init__(self):
		super().__init__()

		self._setDefault(
			outlierTreatment = "asIs",
			lowValue = None,
			highValue = None
		)

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

class Domain(DomainParamsMixin, JavaEstimator, JPMMLReadable, JavaMLWritable):

	_java_class_name = "org.jpmml.sparkml.feature.Domain"

class DomainModel(DomainParamsMixin, JavaTransformer, JPMMLReadable, JavaMLWritable):

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

class CategoricalDomainModel(DomainModel, HasCategoricalDomainParams):

	_java_class_name = "org.jpmml.sparkml.feature.CategoricalDomainModel"

class ContinuousDomain(Domain, HasContinuousDomainParams):

	_java_class_name = "org.jpmml.sparkml.feature.ContinuousDomain"

	def __init__(self, *, java_obj = None, **kwargs):
		if java_obj is None:
			java_obj = _create_java_object(ContinuousDomain._java_class_name)
		super().__init__(java_obj = java_obj)
		self._set(**kwargs)

	def _create_model(self, java_obj):
		return ContinuousDomainModel(java_obj)

class ContinuousDomainModel(DomainModel, HasContinuousDomainParams):

	_java_class_name = "org.jpmml.sparkml.feature.ContinuousDomainModel"

class InvalidCategoryTransformer(JavaTransformer, HasInputCol, HasInputCols, HasOutputCol, HasOutputCols, JPMMLReadable, JavaMLWritable):

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

class VectorDensifier(JavaTransformer, HasInputCol, HasOutputCol, JPMMLReadable, JavaMLWritable):

	_java_class_name = "org.jpmml.sparkml.feature.VectorDensifier"

	def __init__(self, *, java_obj = None, **kwargs):
		if java_obj is None:
			java_obj = _create_java_object(VectorDensifier._java_class_name)
		super().__init__(java_obj = java_obj)
		self._set(**kwargs)

	def setInputCol(self, value):
		return self._set(inputCol = value)

	def setOutputCol(self, value):
		return self._set(outputCol = value)

class SparseToDenseTransformer(VectorDensifier):

	_java_class_name = "org.jpmml.sparkml.feature.SparseToDenseTransformer"

	def __init__(self, *, java_obj = None, **kwargs):
		warnings.warn( "SparseToDenseTransformer is deprecated. Use VectorDensifier instead.", DeprecationWarning, stacklevel = 2)
		if java_obj is None:
			java_obj = _create_java_object(SparseToDenseTransformer._java_class_name)
		super().__init__(java_obj = java_obj, **kwargs)

class VectorDisassembler(JavaTransformer, HasInputCol, HasOutputCols, JPMMLReadable, JavaMLWritable):

	_java_class_name = "org.jpmml.sparkml.feature.VectorDisassembler"

	def __init__(self, *, java_obj = None, **kwargs):
		if java_obj is None:
			java_obj = _create_java_object(VectorDisassembler._java_class_name)
		super().__init__(java_obj = java_obj)
		self._set(**kwargs)

	def setInputCol(self, value):
		return self._set(inputCol = value)

	def setOutputCols(self, value):
		return self._set(outputCols = value)

_register_jpmml_class(CategoricalDomain)
_register_jpmml_class(CategoricalDomainModel)
_register_jpmml_class(ContinuousDomain)
_register_jpmml_class(ContinuousDomainModel)

_register_jpmml_class(InvalidCategoryTransformer)
_register_jpmml_class(VectorDensifier)
_register_jpmml_class(SparseToDenseTransformer)
_register_jpmml_class(VectorDisassembler)