# -*- coding: utf-8 -*-
"""episodes.py test script.

Prompt parameters:
  help (-h):    argparse help
  input (-i):   input path

Examples:
  $ spark-submit \
      --packages com.databricks:spark-avro_2.10:2.0.1 \
      episodes.py \
      --input episodes.avro

"""

import ntpath
from argparse import RawDescriptionHelpFormatter, ArgumentParser
import subprocess

from pyspark import SparkContext
from pyspark.sql import SQLContext

parser = ArgumentParser(description=__doc__,
                        formatter_class=RawDescriptionHelpFormatter)
parser.add_argument('--input', '-i', required=True)
args = parser.parse_args()

if __name__ == "__main__":
    in_path = args.input

    filename = ntpath.basename(in_path)
    subprocess.call(["hadoop", "fs", "-put", in_path, filename])

    sc = SparkContext(appName="Episodes")
    sqlContext = SQLContext(sc)
    df = sqlContext.read.format("com.databricks.spark.avro").load(filename)
    df.first()
