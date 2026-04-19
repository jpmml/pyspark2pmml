from typing import List

import glob
import os

def load_jars(path) -> List[str]:
	return glob.glob(os.path.join(os.path.join(path, "resources"), "*.jar"))
