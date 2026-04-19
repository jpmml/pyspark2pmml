PySpark2PMML
============

Python package for converting [Apache Spark ML](https://spark.apache.org/) pipelines to PMML.

# Features #

This package is a thin PySpark wrapper for the [JPMML-SparkML](https://github.com/jpmml/jpmml-sparkml#features) library.

# News and Updates #

See the [NEWS.md](https://github.com/jpmml/pyspark2pmml/blob/master/NEWS.md) file.

# Prerequisites #

* PySpark 3.0.X through 3.5.X, 4.0.X or 4.1.X.
* Python 3.8 or newer.

# Installation #

Install a release version from PyPI:

```bash
pip install pyspark2pmml
```

Alternatively, install the latest snapshot version from GitHub:

```bash
pip install --upgrade git+https://github.com/jpmml/pyspark2pmml.git
```

# Configuration #

One and the same PySpark2PMML version works across all supported PySpark release lines.
Version variance is confined to the underlying JPMML-SparkML library, where each Apache Spark release line maps to a dedicated JPMML-SparkML release line.

PySpark2PMML must be paired with JPMML-SparkML based on the following compatibility matrix:

| Apache Spark version | JPMML-SparkML branch | Latest JPMML-SparkML version |
|----------------------|----------------------|------------------------------|
| 4.1.X | [`master`](https://github.com/jpmml/jpmml-sparkml/tree/master) | 3.3.3 |
| 4.0.X | [`3.2.X`](https://github.com/jpmml/jpmml-sparkml/tree/3.2.X) | 3.2.10 |
| 3.5.X | [`3.1.X`](https://github.com/jpmml/jpmml-sparkml/tree/3.1.X) | 3.1.11 |
| 3.4.X | [`3.0.X`](https://github.com/jpmml/jpmml-sparkml/tree/3.0.X) | 3.0.11 |

Additionally, PySpark2PMML should be interoperable with now-legacy Apache Spark 3.0 through 3.3 release lines.
Please see the JPMML-SparkML documentation for extended compatibility matrices.

## Local setup

PySpark2PMML version 0.11.0 and newer bundle JPMML-SparkML JAR files for quick programmatic setup.

Use the `pyspark2pmml.spark_jars()` utility function to obtain a PySpark-version dependent classpath string, and pass it as `spark.jars` configuration entry when building a Spark session:

```python
from pyspark.sql import SparkSession

import pyspark2pmml

spark = SparkSession.builder \
	.config("spark.jars", pyspark2pmml.spark_jars()) \
	.getOrCreate()
```

## Cluster setup

Use the `pyspark2pmml.spark_jars_packages()` utility function to obtain a PySpark-version dependent Apache Maven package coordinates string:

```python
import pyspark2pmml

print(pyspark2pmml.spark_jars_packages())
```

Pass this value to `pyspark` or `spark-submit` using the `--packages` command-line option:

```bash
$SPARK_HOME/bin/pyspark --packages $(python -c "import pyspark2pmml; print(pyspark2pmml.spark_jars_packages())")
```

# Usage #

Fitting a Spark ML pipeline:

```python
from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.feature import RFormula

df = spark.read.csv("Iris.csv", header = True, inferSchema = True)

formula = RFormula(formula = "Species ~ .")
classifier = DecisionTreeClassifier()
pipeline = Pipeline(stages = [formula, classifier])
pipelineModel = pipeline.fit(df)
```

Exporting the fitted Spark ML pipeline to a PMML file:

```python
from pyspark2pmml import PMMLBuilder

pmmlBuilder = PMMLBuilder(df.schema, pipelineModel)

pmmlBuilder.buildFile("DecisionTreeIris.pmml")
```

The representation of individual Spark ML pipeline stages can be customized via conversion options:

```python
from pyspark2pmml import PMMLBuilder

classifierModel = pipelineModel.stages[1]

pmmlBuilder = PMMLBuilder(df.schema, pipelineModel) \
	.putOption(classifierModel, "compact", False) \
	.putOption(classifierModel, "estimate_featureImportances", True)

pmmlBuilder.buildFile("DecisionTreeIris.pmml")
```

# License #

PySpark2PMML is licensed under the terms and conditions of the [GNU Affero General Public License, Version 3.0](https://www.gnu.org/licenses/agpl-3.0.html).

If you would like to use PySpark2PMML in a proprietary software project, then it is possible to enter into a licensing agreement which makes PySpark2PMML available under the terms and conditions of the [BSD 3-Clause License](https://opensource.org/licenses/BSD-3-Clause) instead.

# Additional information #

PySpark2PMML is developed and maintained by Openscoring Ltd, Estonia.

Interested in using [Java PMML API](https://github.com/jpmml) software in your company? Please contact [info@openscoring.io](mailto:info@openscoring.io)
