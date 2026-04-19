from typing import List

import glob
import os

def load_jars(path) -> List[str]:
	return glob.glob(os.path.join(os.path.join(path, "resources"), "*.jar"))

def load_jars_packages(path) -> List[str]:
	packages_file = os.path.join(path, "resources", "packages.txt")
	with open(packages_file) as f:
		return f.read().splitlines()
