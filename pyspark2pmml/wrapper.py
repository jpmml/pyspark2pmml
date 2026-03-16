from pyspark import SparkContext
from pyspark.ml.util import MLReadable, MLReader

import sys
import types

def _jvm():
	return SparkContext._jvm

def _ensure_module(module_path):
	segments = module_path.split(".")
	for i in range(len(segments)):
		path = ".".join(segments[:i + 1])
		if path not in sys.modules:
			sys.modules[path] = types.ModuleType(path)
		if i > 0:
			parent_path = ".".join(segments[:i])
			setattr(sys.modules[parent_path], segments[i], sys.modules[path])
	return sys.modules[module_path]

def _register_jpmml_class(py_class):
	java_class_name = py_class._java_class_name
	parts = java_class_name.rsplit(".", 1)
	module = _ensure_module(parts[0])
	if not hasattr(module, parts[1]):
		setattr(module, parts[1], py_class)

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

class JPMMLReadable(MLReadable):

	@classmethod
	def read(cls):
		return _JavaReader(cls, cls._java_class_name)

class _JavaReader(MLReader):

	def __init__(self, py_class, java_class_name):
		super().__init__()
		self.py_class = py_class
		self.java_class_name = java_class_name

	def load(self, path):
		java_obj = getattr(_jvm(), self.java_class_name).load(path)

		py_obj = self.py_class(java_obj = java_obj)
		py_obj._resetUid(java_obj.uid())
		py_obj._transfer_params_from_java()

		return py_obj
