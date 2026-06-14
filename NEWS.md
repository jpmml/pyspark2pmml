# 0.11.1 #

## Breaking changes

None.

## New features

* Added record counts and intermediate leaf scores for XGBoost models.

This functionality is available only if the underlying objective function supports it (eg. fully supported by `reg:squarederror`, partially supported by `reg:absoluteerror` and `reg:squaredlogerror`).

Moreover, the record counts and intermediate leaf scores only make sense in reference to the original (ie. binary tree) layout of XGBoost models.

For example, exporting the same XGBoost model first in native-looking (deep, with maximum metadata) and then in optimized (compacted, without metadata) representations:

```python
from pyspark2pmml import PMMLBuilder

xgbModel = xgbPipelineModel.stages[-1]

pmmlBuilder = PMMLBuilder(df.schema, xgbPipelineModel)

# Native-looking representation for analysis and interpretation
pmmlBuilder \
	.putOption(xgbModel, "compact", False) \
	.buildFile("XGBoost_native.pmml")

# Optimized representation for scoring
pmmlBuilder \
	.putOption(xgbModel, "compact", True) \
	.buildFile("XGBoost_optimized.pmml")
```

* Unwrapped the `TreeModel` element when encoding single-segment LightGBM and XGBoost models.

## Minor improvements and fixes

* Updated JPMML-SparkML library versions.

# 0.11.0 #

## Breaking changes

None.

## New features

* Bundled JPMML-SparkML library JARs for Apache Spark 3.4, 3.5, 4.0 and 4.1.
The package is now fully self-contained - no need to manually locate, download and manage JPMML-SparkML dependencies anymore.

* Added `pyspark2pmml.spark_jars()` utility function for local programmatic setup.
Returns a `spark.jars`-compatible classpath string for the current PySpark version.

```python
from pyspark.sql import SparkSession

import pyspark2pmml

spark = SparkSession.builder \
	.config("spark.jars", pyspark2pmml.spark_jars()) \
	.getOrCreate()
```

* Added `pyspark2pmml.spark_jars_packages()` utility function for cluster setup.
Returns a `spark.jars.packages`-compatible Apache Maven package coordinates string for the current PySpark version.

## Minor improvements and fixes

None.


# 0.10.0 #

## Breaking changes

* Refactored the `pyspark2pmml.PMMLBuilder` constructor:
  * Removed the (previously leading-) `sc: SparkContext` parameter.
  * Renamed and refactored the `df: DataFrame` parameter to `schema: Union[StructType, DataFrame]`. The converter needs to know the schema of the input dataset, not its data. Fetch the default schema object from the training dataset as `DataFrame.schema`. Adjust field names and data types manually as needed.
  * Renamed and relaxed the `pipelineModel: PipelineModel` parameter to `pipelineStage: Transformer`. The converter can process any PySpark transformer, not just pipeline models.

Before:

```python
pmmlBuilder = PMMLBuilder(sc, df, pipelineModel)
```

The same now:

```python
schema = df.schema

pmmlBuilder = PMMLBuilder(schema, pipelineModel)
```

* Removed the deprecated `pyspark2pmml.toPMMLBytes()` utility function.

## New features

None.

## Minor improvements and fixes

None.
