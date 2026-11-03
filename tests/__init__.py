"""
Unit test suite for Medallion PySpark pipeline.
"""

import os
import sys

# Ensure all PySpark workers across the entire test suite use the current Python interpreter on Windows
if "PYSPARK_PYTHON" not in os.environ:
    os.environ["PYSPARK_PYTHON"] = sys.executable
if "PYSPARK_DRIVER_PYTHON" not in os.environ:
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
