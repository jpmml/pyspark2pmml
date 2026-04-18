from pyspark import SparkContext
from pyspark.ml.util import MLReadable, MLReader
from pyspark2pmml import shared, spark34, spark35, spark40, spark41
from types import ModuleType

import sys

def _jvm():
	return SparkContext._jvm

def _ensure_module(module_path):
	segments = module_path.split(".")
	for i in range(len(segments)):
		path = ".".join(segments[:i + 1])
		if path not in sys.modules:
			sys.modules[path] = ModuleType(path)
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

def _create_java_object(java_class_name, *args):
	return getattr(_jvm(), java_class_name)(*args)

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
