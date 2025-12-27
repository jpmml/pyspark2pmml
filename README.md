PySpark2PMML
============

Python package for converting [Apache Spark ML](https://spark.apache.org/) pipelines to PMML.

# Features #

This package is a thin PySpark wrapper for the [JPMML-SparkML](https://github.com/jpmml/jpmml-sparkml#features) library.

# Prerequisites #

* Apache Spark 3.0.X, 3.1.X, 3.2.X, 3.3.X, 3.4.X, 3.5.X, 4.0.X or 4.1.X.
* Python 3.9 or newer.

# Installation #

Install a release version from PyPI:

```
pip install pyspark2pmml
```

Alternatively, install the latest snapshot version from GitHub:

```
pip install --upgrade git+https://github.com/jpmml/pyspark2pmml.git
```

# Configuration and usage #

PySpark2PMML must be paired with JPMML-SparkML based on the following compatibility matrix:

Active development branches:

| Apache Spark version | JPMML-SparkML branch | Latest JPMML-SparkML version |
|----------------------|----------------------|------------------------------|
| 3.4.X | [`3.0.X`](https://github.com/jpmml/jpmml-sparkml/tree/3.0.X) | 3.0.8 |
| 3.5.X | [`3.1.X`](https://github.com/jpmml/jpmml-sparkml/tree/3.1.X) | 3.1.8 |
| 4.0.X | [`3.2.X`](https://github.com/jpmml/jpmml-sparkml/tree/3.2.X) | 3.2.7 |
| 4.1.X | [`master`](https://github.com/jpmml/jpmml-sparkml/tree/master) | 3.3.0 |

Stale development branches:

| Apache Spark version | JPMML-SparkML branch | Latest JPMML-SparkML version |
|----------------------|----------------------|------------------------------|
| 3.0.X | [`2.0.X`](https://github.com/jpmml/jpmml-sparkml/tree/2.0.X) | 2.0.6 |
| 3.1.X | [`2.1.X`](https://github.com/jpmml/jpmml-sparkml/tree/2.1.X) | 2.1.6 |
| 3.2.X | [`2.2.X`](https://github.com/jpmml/jpmml-sparkml/tree/2.2.X) | 2.2.6 |
| 3.3.X | [`2.3.X`](https://github.com/jpmml/jpmml-sparkml/tree/2.3.X) | 2.3.5 |
| 3.4.X | [`2.4.X`](https://github.com/jpmml/jpmml-sparkml/tree/2.4.X) | 2.4.4 |
| 3.5.X | [`2.5.X`](https://github.com/jpmml/jpmml-sparkml/tree/2.5.X) | 2.5.3 |

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

pmmlBuilder = PMMLBuilder(sc, df, pipelineModel)

pmmlBuilder.buildFile("DecisionTreeIris.pmml")
```

The representation of individual Spark ML pipeline stages can be customized via conversion options:

```python
from pyspark2pmml import PMMLBuilder

classifierModel = pipelineModel.stages[1]

pmmlBuilder = PMMLBuilder(sc, df, pipelineModel) \
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
