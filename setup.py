from setuptools import setup

exec(open('pyspark2pmml/metadata.py').read())

setup(
	name = "pyspark2pmml",
	version = __version__,
	description = "Python library for converting Apache Spark ML pipelines to PMML",
	author = "Villu Ruusmann",
	author_email = "villu.ruusmann@gmail.com",
	url = "https://github.com/jpmml/pyspark2pmml",
	license = __license__,
	packages = [
		"pyspark2pmml"
	],
	install_requires = [
		"py4j"
	]
)
