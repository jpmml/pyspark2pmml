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

```
pip install pyspark2pmml
```

Alternatively, install the latest snapshot version from GitHub:

```
pip install --upgrade git+https://github.com/jpmml/pyspark2pmml.git
```

# Configuration #

One and the same PySpark2PMML version works across all supported PySpark release lines.
Version variance is confined to the underlying JPMML-SparkML library, where each Apache Spark release line maps to a dedicated JPMML-SparkML release line.

PySpark2PMML must be paired with JPMML-SparkML based on the following compatibility matrix:

Active development branches:

| Apache Spark version | JPMML-SparkML branch | Latest JPMML-SparkML version |
|----------------------|----------------------|------------------------------|
| 3.4.X | [`3.0.X`](https://github.com/jpmml/jpmml-sparkml/tree/3.0.X) | 3.0.10 |
| 3.5.X | [`3.1.X`](https://github.com/jpmml/jpmml-sparkml/tree/3.1.X) | 3.1.10 |
| 4.0.X | [`3.2.X`](https://github.com/jpmml/jpmml-sparkml/tree/3.2.X) | 3.2.9 |
| 4.1.X | [`master`](https://github.com/jpmml/jpmml-sparkml/tree/master) | 3.3.2 |

Stale development branches:

| Apache Spark version | JPMML-SparkML branch | Latest JPMML-SparkML version |
|----------------------|----------------------|------------------------------|
| 3.0.X | [`2.0.X`](https://github.com/jpmml/jpmml-sparkml/tree/2.0.X) | 2.0.6 |
| 3.1.X | [`2.1.X`](https://github.com/jpmml/jpmml-sparkml/tree/2.1.X) | 2.1.6 |
| 3.2.X | [`2.2.X`](https://github.com/jpmml/jpmml-sparkml/tree/2.2.X) | 2.2.6 |
| 3.3.X | [`2.3.X`](https://github.com/jpmml/jpmml-sparkml/tree/2.3.X) | 2.3.5 |
| 3.4.X | [`2.4.X`](https://github.com/jpmml/jpmml-sparkml/tree/2.4.X) | 2.4.4 |
| 3.5.X | [`2.5.X`](https://github.com/jpmml/jpmml-sparkml/tree/2.5.X) | 2.5.3 |

PySpark2PMML Python APIs are simple and stable in time.
If the package has not been updated for months or even a year, then this does not mean that it has fallen behind JPMML-SparkML development in any way.

Quite the contrary.
The latest PySpark2PMML package version should be fully interoperable with any and all JPMML-SparkML library versions that have been released since that time.

# Usage #

Launch PySpark; use the `--packages` command-line option to specify the coordinates of relevant JPMML-SparkML modules:

* `org.jpmml:pmml-sparkml:${version}` - Core module.
* `org.jpmml:pmml-sparkml-lightgbm:${version}` - LightGBM via SynapseML extension module.
* `org.jpmml:pmml-sparkml-xgboost:${version}` - XGBoost via XGBoost4J-Spark extension module.

Launching core:

```
pyspark --packages org.jpmml:pmml-sparkml:${version}
```

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
