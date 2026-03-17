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
