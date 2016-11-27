JPMML-SparkML-Package
=====================

JPMML-SparkML as an [Apache Spark Package] (https://spark-packages.org/).

# Prerequisites #

* [Apache Spark] (http://spark.apache.org/) 1.6.X.

# Installation #

Clone the JPMML-SparkML-Package project and enter its directory:
```
git clone https://github.com/jpmml/jpmml-sparkml-package.git
cd jpmml-sparkml-package
```

Check out the `spark-1.6.X` development branch:
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

Test the uber-JAR file:
```
cd src/main/python
nosetests
```

# Usage #

### Scala ###

Launch the Spark shell with JPMML-SparkML-Package; use `--jars` to specify the location of the uber-JAR file:
```
spark-shell --jars /path/to/jpmml-sparkml-package/target/jpmml-sparkml-package-1.0-SNAPSHOT.jar
```

Fitting an example pipeline model:
```scala
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.feature.RFormula

// Requires "--packages com.databricks:spark-csv_2.10:1.5.0"
val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("Iris.csv")

val formula = new RFormula().setFormula("Species ~ .")
val classifier = new DecisionTreeClassifier()
val pipeline = new Pipeline().setStages(Array(formula, classifier))
val pipelineModel = pipeline.fit(df)
```

Exporting the fitted example pipeline model to PMML byte array:
```scala
val pmmlBytes = org.jpmml.sparkml.ConverterUtil.toPMMLByteArray(df.schema, pipelineModel)
println(new String(pmmlBytes, "UTF-8"))
```

### PySpark ###

Add the EGG file to the `PYTHONPATH` environment variable:
```
export PYTHONPATH=$PYTHONPATH:/path/to/jpmml-sparkml-package/target/jpmml_sparkml-1.0rc0.egg
```

Launch the PySpark shell with JPMML-SparkML-Package; use `--jars` and `--driver-class-path` to specify the location of the uber-JAR file:
```
pyspark --jars /path/to/jpmml-sparkml-package/target/jpmml-sparkml-package-1.0-SNAPSHOT.jar --driver-class-path /path/to/jpmml-sparkml-package/target/jpmml-sparkml-package-1.0-SNAPSHOT.jar
```

Fitting an example pipeline model:
```python
from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.feature import RFormula

# Requires "--packages com.databricks:spark-csv_2.10:1.5.0"
df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferschema", "true").load("Iris.csv")

formula = RFormula(formula = "Species ~ .")
classifier = DecisionTreeClassifier()
pipeline = Pipeline(stages = [formula, classifier])
pipelineModel = pipeline.fit(df)
```

Exporting the fitted example pipeline model to PMML byte array:
```python
from jpmml_sparkml import toPMMLBytes

pmmlBytes = toPMMLBytes(sc, df, pipelineModel)
print(pmmlBytes.decode("UTF-8"))
```

# License #

JPMML-SparkML-Package is licensed under the [GNU Affero General Public License (AGPL) version 3.0] (http://www.gnu.org/licenses/agpl-3.0.html). Other licenses are available on request.

# Additional information #

Please contact [info@openscoring.io] (mailto:info@openscoring.io)
