PySpark2PMML
============

Python library for converting Apache Spark ML pipelines to PMML.

# Prerequisites #

* [Apache Spark](http://spark.apache.org/) 2.0.X, 2.1.X, 2.2.X or 2.3.X.

# Installation #

Clone the PySpark2PMML project and enter its directory:
```
git clone https://github.com/jpmml/pyspark2pmml.git
cd pyspark2pmml
```

The repository contains a number development branches:

| Branch | Apache Spark version | PySpark2PMML version |
|--------|----------------------|----------------------|
| `spark-2.3.X` | 2.3.X | 1.4(-SNAPSHOT) |
| `spark-2.2.X` | 2.2.X | 1.3(-SNAPSHOT) |
| `spark-2.1.X` | 2.1.X | 1.2(-SNAPSHOT) |
| `spark-2.0.X` | 2.0.X | 1.1(-SNAPSHOT) |

Check out the correct development branch. For example, when targeting Apache Spark 2.2.X, check out the `spark-2.2.X` development branch:
```
git checkout spark-2.2.X
```

Add the Python bindings of Apache Spark to the `PYTHONPATH` environment variable:
```
export PYTHONPATH=$PYTHONPATH:$SPARK_HOME/python
```

Build the project using [Apache Maven](http://maven.apache.org/); use the `python.exe` system property to specify the location of the Python executable (eg. switching between Python 2.X and 3.X executables):
```
mvn -Dpython.exe=/usr/bin/python3.4 clean package
```

The build produces an EGG file `target/pyspark2pmml-1.3rc0.egg` and an uber-JAR file `target/pyspark2pmml-1.3-SNAPSHOT.jar`.

Test the uber-JAR file:
```
nosetests
```

# Usage #

Add the EGG file to the `PYTHONPATH` environment variable:
```
export PYTHONPATH=$PYTHONPATH:/path/to/pyspark2pmml/target/pyspark2pmml-1.3rc0.egg
```

Launch the PySpark shell with PySpark2PMML; use `--jars` to specify the location of the uber-JAR file:
```
pyspark --jars /path/to/pyspark2pmml/target/pyspark2pmml-1.3-SNAPSHOT.jar
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

Exporting the fitted example pipeline model to PMML byte array:
```python
from pyspark2pmml import PMMLBuilder

pmmlBuilder = PMMLBuilder(sc, df, pipelineModel) \
	.putOption(classifier, "compact", True)

pmmlBytes = pmmlBuilder.buildByteArray()
print(pmmlBytes.decode("UTF-8"))
```

# License #

PySpark2PMML is dual-licensed under the [GNU Affero General Public License (AGPL) version 3.0](http://www.gnu.org/licenses/agpl-3.0.html), and a commercial license.

# Additional information #

PySpark2PMML is developed and maintained by Openscoring Ltd, Estonia.

Interested in using JPMML software in your application? Please contact [info@openscoring.io](mailto:info@openscoring.io)
