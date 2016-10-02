JPMML-SparkML-Package
=====================

JPMML-SparkML as an [Apache Spark Package] (https://spark-packages.org/).

# Prerequisites #

* [Apache Spark] (http://spark.apache.org/) 1.5.X, 1.6.X or 2.0.X.

# Installation #

Clone the JPMML-SparkML-Package project and enter its directory:
```
git clone https://github.com/jpmml/jpmml-sparkml-package.git
cd jpmml-sparkml-package
```

When targeting Apache Spark 1.5.X or 1.6.X, check out the `spark-1.6.X` development branch:
```
git checkout spark-1.6.X
```

### Scala ###

Build the project:
```
mvn clean package
```

The build produces an uber-JAR file `target/jpmml-sparkml-package-1.0-SNAPSHOT.jar`.

### PySpark ###

Add the Python bindings of Apache Spark to the `PYTHONPATH` environment variable:
```
export PYTHONPATH=$PYTHONPATH:$SPARK_HOME/python
```

Build the project using the `pyspark` profile:
```
mvn -Ppyspark clean package
```

The build produces an EGG file `target/jpmml_sparkml-1.0rc0.egg` and an uber-JAR file `target/jpmml-sparkml-package-1.0-SNAPSHOT.jar`.

# Usage #

### Scala ###

Launch the Spark shell with JPMML-SparkML-Package; use `--jars` to specify the location of the uber-JAR file:
```
spark-shell --jars /path/to/jpmml-sparkml-package/target/jpmml-sparkml-package-1.0-SNAPSHOT.jar 
```

Fitting an example pipeline model:
```scala
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.ml.regression.DecisionTreeRegressor

val data = spark.read.option("header", "true").option("inferSchema", "true").csv("wine.csv")

val formula = new RFormula().setFormula("quality ~ .")
val regressor = new DecisionTreeRegressor()
val pipeline = new Pipeline().setStages(Array(formula, regressor))
val pipelineModel = pipeline.fit(data)
```

Exporting the fitted example pipeline model to PMML byte array:
```scala
val pmmlBytes = org.jpmml.sparkml.ConverterUtil.toPMMLByteArray(data.schema, pipelineModel)
println(new String(pmmlBytes, "UTF-8"))
```

### PySpark ###

Add the EGG file to the `PYTHONPATH` environment variable:
```
export PYTHONPATH=$PYTHONPATH:/path/to/jpmml-sparkml-package/target/jpmml_sparkml-1.0rc0.egg
```

Launch the PySpark shell with JPMML-SparkML-Package; use `--jars` to specify the location of the uber-JAR file:
```
pyspark --jars /path/to/jpmml-sparkml-package/target/jpmml-sparkml-package-1.0-SNAPSHOT.jar
```

Fitting an example pipeline model:
```python
from pyspark.ml import Pipeline
from pyspark.ml.feature import RFormula
from pyspark.ml.regression import DecisionTreeRegressor

data = spark.read.csv("wine.csv", header = True, inferSchema = True)

formula = RFormula(formula = "quality ~ .")
regressor = DecisionTreeRegressor()
pipeline = Pipeline(stages = [formula, regressor])
pipelineModel = pipeline.fit(data)
```

Exporting the fitted example pipeline model to PMML byte array:
```python
from jpmml_sparkml import toPMMLBytes

pmmlBytes = toPMMLBytes(sc, data, pipelineModel)
print(pmmlBytes)
```

# License #

JPMML-SparkML-Package is licensed under the [GNU Affero General Public License (AGPL) version 3.0] (http://www.gnu.org/licenses/agpl-3.0.html). Other licenses are available on request.

# Additional information #

Please contact [info@openscoring.io] (mailto:info@openscoring.io)
