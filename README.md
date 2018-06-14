PySpark2PMML
============

Python library for converting Apache Spark ML pipelines to PMML.

# Features #

This library is a thin wrapper around the [JPMML-SparkML](https://github.com/jpmml/jpmml-sparkml) library. For a list of supported Apache Spark ML Estimator and Transformer types, please refer to the documentation of the JPMML-SparkML project.

# Prerequisites #

* [Apache Spark](http://spark.apache.org/) 2.0.X, 2.1.X, 2.2.X or 2.3.X.
* Python 2.7, 3.4 or newer.


# Installation #

Install the latest version from GitHub:
```
pip install --user --upgrade git+https://github.com/jpmml/pyspark2pmml.git
```

# Configuration #

PySpark2PMML must be paired with JPMML-SparkML based on the following compatibility matrix:

| Apache Spark version | JPMML-SparkML development branch | JPMML-SparkML version |
|----------------------|----------------------------------|-----------------------|
| 2.0.X | `1.1.X` | 1.1.19 |
| 2.1.X | `1.2.X` | 1.2.11 |
| 2.2.X | `1.3.X` | 1.3.7 |
| 2.3.X | `master` | 1.4.4 |

### Apache Spark 2.3.X

Launch PySpark; use the `--packages` command-line option to specify the Maven Central repository coordinates of the JPMML-SparkML library:
```
pyspark --packages org.jpmml:jpmml-sparkml:${version}
```

### Apache Spark 2.0.X through 2.2.X

Apache Spark versions prior to 2.3.0 prepend a legacy version of the JPMML-Model library to application classpath, which brings about fatal class loading errors with all JPMML software, including the JPMML-SparkML library. This conflict is documented in [SPARK-15526](https://issues.apache.org/jira/browse/SPARK-15526).

The workaround is to switch from the JPMML-SparkML library to the JPMML-SparkML uber-JAR file that bundles "shaded" classes. Currently, this JPMML-SparkML uber-JAR file needs to be built locally using [Apache Maven](http://maven.apache.org/):
```
git clone https://github.com/jpmml/jpmml-sparkml.git
cd jpmml-sparkml
# Check out the intended development branch
git checkout ${development branch}
mvn clean package
```

Launch PySpark; use the `--jars` command-line option to specify the location of the JPMML-SparkML uber-JAR file:
```
pyspark --jars /path/to/jpmml-sparkml/target/converter-executable-${version}.jar
```

# Usage #

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
