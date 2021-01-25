PySpark2PMML
============

Python library for converting Apache Spark ML pipelines to PMML.

# Features #

This package provides Python wrapper classes and functions for the [JPMML-SparkML](https://github.com/jpmml/jpmml-sparkml) library. For the full list of supported Apache Spark ML Estimator and Transformer types, please refer to JPMML-SparkML documentation.

# Prerequisites #

* Apache Spark 2.0.X, 2.1.X, 2.2.X, 2.3.X, 2.4.X or 3.0.X.
* Python 2.7, 3.4 or newer.

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

| Apache Spark version | JPMML-SparkML branch | JPMML-SparkML uber-JAR file |
|----------------------|----------------------|-----------------------------|
| 2.0.X | `1.1.X` (Archived) | [1.1.23](https://github.com/jpmml/jpmml-sparkml/releases/download/1.1.23/jpmml-sparkml-executable-1.1.23.jar) |
| 2.1.X | `1.2.X` (Archived) | [1.2.15](https://github.com/jpmml/jpmml-sparkml/releases/download/1.2.15/jpmml-sparkml-executable-1.2.15.jar) |
| 2.2.X | `1.3.X` (Archived) | [1.3.15](https://github.com/jpmml/jpmml-sparkml/releases/download/1.3.15/jpmml-sparkml-executable-1.3.15.jar) |
| 2.3.X | `1.4.X` | [1.4.18](https://github.com/jpmml/jpmml-sparkml/releases/download/1.4.18/jpmml-sparkml-executable-1.4.18.jar) |
| 2.4.X | `1.5.X` | [1.5.11](https://github.com/jpmml/jpmml-sparkml/releases/download/1.5.11/jpmml-sparkml-executable-1.5.11.jar) |
| 3.0.X | `master` | [1.6.3](https://github.com/jpmml/jpmml-sparkml/releases/download/1.6.3/jpmml-sparkml-executable-1.6.3.jar) |

Launch PySpark; use the `--jars` command-line option to specify the location of the JPMML-SparkML uber-JAR file:
```
pyspark --jars /path/to/jpmml-sparkml-executable-${version}.jar
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
