from xgboost.spark import SparkXGBClassifierModel, SparkXGBRegressorModel

import tempfile
import types

def patch_model(sc, model):
	if hasattr(model, "_to_java"):
		return

	javaModel = toJavaModel(sc, model)

	def _to_java(self):
		return javaModel

	model._to_java = types.MethodType(_to_java, model)

def toJavaModel(sc, model):
	if isinstance(model, SparkXGBClassifierModel):
		sklearnModel = model._xgb_sklearn_model
		javaBooster = toJavaBooster(sc, sklearnModel.get_booster())
		javaModel = sc._jvm.ml.dmlc.xgboost4j.scala.spark.XGBoostClassificationModel(
			model.uid,
			sklearnModel.n_classes_, # XXX
			javaBooster
		) \
			.setFeaturesCol(model.getFeaturesCol()) \
			.setPredictionCol(model.getPredictionCol()) \
			.setProbabilityCol(model.getProbabilityCol())
		return javaModel
	elif isinstance(model, SparkXGBRegressorModel):
		sklearnModel = model._xgb_sklearn_model
		javaBooster = toJavaBooster(sc, sklearnModel.get_booster())
		javaModel = sc._jvm.ml.dmlc.xgboost4j.scala.spark.XGBoostRegressionModel(
			model.uid,
			javaBooster
		) \
			.setFeaturesCol(model.getFeaturesCol()) \
			.setPredictionCol(model.getPredictionCol())
		return javaModel
	else:
		raise TypeError()

def toJavaBooster(sc, booster):
	with tempfile.NamedTemporaryFile(suffix = ".json") as booster_file:
		booster_path = booster_file.name
		booster.save_model(booster_path)
		return sc._jvm.ml.dmlc.xgboost4j.scala.XGBoost.loadModel(booster_path)
