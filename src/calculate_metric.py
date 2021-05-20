import math
from typing import Dict, List
from src.common import spark, LOGGER
from datetime import timedelta
from pyspark.sql.types import *
from pyspark.sql.functions import (
    col, 
    ceil, 
    datediff, 
    udf
)

def process(
    input_path_registered: str,
    input_path_loaded: str
):
    """
    :param: input_path_registered: Input path for registered users
    :param: input_path_loaded: Input path for app loaded information
    """

    # UDF function to filter calendar week difference equals to 1
    @udf(BooleanType())
    def week_diff(registered_date, loaded_date):
        monday_reg = (registered_date - timedelta(days=registered_date.weekday()))
        monday_load = (loaded_date - timedelta(days=loaded_date.weekday()))
        diff = monday_load - monday_reg
        num_weeks = math.ceil(diff.days / 7) 
        return num_weeks == 1

    try:

        df_registered = spark.read.parquet(input_path_registered)
        df_loaded = spark.read.parquet(input_path_loaded)

        # Join data from registered users and app usage and use week_diff udf
        # Then filter the unique users and count them
        users_week_diff = df_registered.join(
            df_loaded, ["initiator_id"], "inner").where(
                week_diff(df_registered["time"], df_loaded["time"])
        ).select("initiator_id").distinct().count()

        # Total number of registered users
        total_users = df_registered.select("initiator_id").distinct().count()

        # Return percentage of users
        return "{:.2f}".format(users_week_diff*100/total_users)
    
    except Exception as e:
        LOGGER.exception(f"Events metric calculation: {e}")