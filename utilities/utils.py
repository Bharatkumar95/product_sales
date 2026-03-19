from pyspark.sql.functions import udf
from pyspark.sql.functions import *
from pyspark.sql.types import BooleanType, TimestampType
import re
from datetime import datetime

@udf(returnType=TimestampType())
def get_current_ts():
    return datetime.now()
