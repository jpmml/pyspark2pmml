from xgboost.spark import SparkXGBClassifierModel, SparkXGBRegressorModel
from pyspark2pmml.wrapper import _jvm

import tempfile
import types

def patch_model(model):
	if hasattr(model, "_to_java"):
		return

	javaModel = toJavaModel(model)

	def _to_java(self):
		return javaModel

	model._to_java = types.MethodType(_to_java, model)

def toJavaModel(model):
	jvm = _jvm()

	def _construct(javaModelClass, args):
		# XGBoost 2.X
		try:
			javaModel = javaModelClass(*args)
		# XGBoost 3.X
		except:
			javaModel = javaModelClass(*(args + [None]))
		return javaModel

	if isinstance(model, SparkXGBClassifierModel):
		sklearnModel = model._xgb_sklearn_model
		num_classes = sklearnModel.n_classes_ # XXX
		javaBooster = toJavaBooster(sklearnModel.get_booster())
		javaModelClass = jvm.ml.dmlc.xgboost4j.scala.spark.XGBoostClassificationModel
		javaModel = _construct(javaModelClass, [
			model.uid,
			num_classes,
			javaBooster
		]) \
			.setFeaturesCol(model.getFeaturesCol()) \
			.setPredictionCol(model.getPredictionCol()) \
			.setProbabilityCol(model.getProbabilityCol())
		if javaModel.hasParam("numClass") and num_classes > 2:
			javaModel.set(javaModel.getParam("numClass"), num_classes)
		javaModel.set(javaModel.getParam("labelCol"), model.getLabelCol())
		return javaModel
	elif isinstance(model, SparkXGBRegressorModel):
		sklearnModel = model._xgb_sklearn_model
		javaBooster = toJavaBooster(sklearnModel.get_booster())
		javaModelClass = jvm.ml.dmlc.xgboost4j.scala.spark.XGBoostRegressionModel
		javaModel = _construct(javaModelClass, [
			model.uid,
			javaBooster
		]) \
			.setFeaturesCol(model.getFeaturesCol()) \
			.setPredictionCol(model.getPredictionCol())
		javaModel.set(javaModel.getParam("labelCol"), model.getLabelCol())
		return javaModel
	else:
		raise TypeError()

def toJavaBooster(booster):
	jvm = _jvm()
	with tempfile.NamedTemporaryFile(suffix = ".json") as booster_file:
		booster_path = booster_file.name
		booster.save_model(booster_path)
		return jvm.ml.dmlc.xgboost4j.scala.XGBoost.loadModel(booster_path)
