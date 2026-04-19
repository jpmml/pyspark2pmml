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
