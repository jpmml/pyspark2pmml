PySpark2PMML
============

Python library for converting Apache Spark ML pipelines to PMML.

# Features #

This package provides Python wrapper classes and functions for the [JPMML-SparkML](https://github.com/jpmml/jpmml-sparkml) library. For the full list of supported Apache Spark ML Estimator and Transformer types, please refer to JPMML-SparkML documentation.

# Prerequisites #

* [Apache Spark](http://spark.apache.org/) 2.0.X, 2.1.X, 2.2.X or 2.3.X.
* Python 2.7, 3.4 or newer.

# Installation #

Install the latest version from GitHub:
```
pip install --user --upgrade git+https://github.com/jpmml/pyspark2pmml.git
```

# Configuration and usage #

PySpark2PMML must be paired with JPMML-SparkML based on the following compatibility matrix:

| Apache Spark version | JPMML-SparkML development branch | JPMML-SparkML uber-JAR file |
|----------------------|----------------------------------|-----------------------------|
| 2.0.X | `1.1.X` | [1.1.21](https://github.com/jpmml/jpmml-sparkml/releases/download/1.1.21/jpmml-sparkml-executable-1.1.21.jar) |
| 2.1.X | `1.2.X` | [1.2.13](https://github.com/jpmml/jpmml-sparkml/releases/download/1.2.13/jpmml-sparkml-executable-1.2.13.jar) |
| 2.2.X | `1.3.X` | [1.3.9](https://github.com/jpmml/jpmml-sparkml/releases/download/1.3.9/jpmml-sparkml-executable-1.3.9.jar) |
| 2.3.X | `master` | [1.4.6](https://github.com/jpmml/jpmml-sparkml/releases/download/1.4.6/jpmml-sparkml-executable-1.4.6.jar) |

Launch PySpark; use the `--jars` command-line option to specify the location of the JPMML-SparkML uber-JAR file:
```
pyspark --jars /path/to/jpmml-sparkml-executable-${version}.jar
```

Fitting an example pipeline model:

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

Exporting the fitted example pipeline model to a PMML file:

```python
from pyspark2pmml import PMMLBuilder

pmmlBuilder = PMMLBuilder(sc, df, pipelineModel) \
	.putOption(classifier, "compact", True)

pmmlBuilder.buildFile("DecisionTreeIris.pmml")
```

# License #

PySpark2PMML is dual-licensed under the [GNU Affero General Public License (AGPL) version 3.0](http://www.gnu.org/licenses/agpl-3.0.html), and a commercial license.

# Additional information #

PySpark2PMML is developed and maintained by Openscoring Ltd, Estonia.

Interested in using JPMML software in your application? Please contact [info@openscoring.io](mailto:info@openscoring.io)
