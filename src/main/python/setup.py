from setuptools import setup

from jpmml_sparkml import __license__, __version__

setup(
	name = "jpmml_sparkml",
	version = __version__,
	description = "Python library for converting Apache Spark ML pipelines to PMML",
	author = "Villu Ruusmann",
	author_email = "villu.ruusmann@gmail.com",
	url = "https://github.com/jpmml/jpmml-sparkml-package",
	license = __license__,
	packages = [
		"jpmml_sparkml"
	],
	install_requires = [
		"py4j"
	]
)
