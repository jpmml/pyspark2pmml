from setuptools import setup, find_packages

exec(open('pyspark2pmml/metadata.py').read())

setup(
	name = "pyspark2pmml",
	version = __version__,
	description = "Python library for converting Apache Spark ML pipelines to PMML",
	author = "Villu Ruusmann",
	author_email = "villu.ruusmann@gmail.com",
	url = "https://github.com/jpmml/pyspark2pmml",
	download_url = "https://github.com/jpmml/pyspark2pmml/archive/" + __version__ + ".tar.gz",
	license = __license__,
	classifiers = [
		"Development Status :: 5 - Production/Stable",
		"Operating System :: OS Independent",
		"Programming Language :: Python",
		"Intended Audience :: Developers",
		"Intended Audience :: Science/Research",
		"Topic :: Software Development",
		"Topic :: Scientific/Engineering"
	],
	packages = find_packages(exclude = ["*.tests"]),
	exclude_package_data = {
		"" : ["README.md"],
	},
	python_requires = ">=3.6",
	install_requires = [
		"py4j"
	]
)
