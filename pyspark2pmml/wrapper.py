from pyspark.ml.util import MLReader
from pyspark.sql import SparkSession

def _jvm():
	spark = SparkSession.getActiveSession()
	if spark is None:
		raise RuntimeError("Apache Spark session not found")
	return spark._jvm

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

class JPMMLReadable:

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
