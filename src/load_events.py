from typing import Dict, List
from src.common import spark, LOGGER
from pyspark.sql.types import *
from pyspark.sql.functions import col, to_timestamp

types_dict = {
    TimestampType : TimestampType(),
    StringType : StringType(),
    LongType : LongType()
}

def process(
    input_path,
    output_path,
    events,
    meta_info
) -> None:
    """
    :param: input_path: Path where the json file is located
    :param: output_path: Path where the parquet files corresponding to the events will be saved
    :param: events: Events to be saved and list with required columns for each event
    :param: meta_info: information regarding the events especs, inlcuding field name and type
    """

    LOGGER.info("Starting loadings events on disk")

    try:
        df = spark.read.json(input_path)
        df = df.withColumnRenamed("timestamp", "time")
        df = dataValidation(df, meta_info)

        for k,v in events.items():
            #Filter the required fields for each type of event based on the given dict
            df_events = df.filter(col("event") == k).select(*v)
            df_events.write.mode("overwrite").parquet(output_path + k)
    
    except Exception as e:
        LOGGER.exception(f"Events load failed: {e}")

def dataValidation(df, meta_info: List[Dict]):
    """
    :param: df: df that requires schema validation
    :param: meta_info: information regarding the events especs, inlcuding field name and type
    """

    try: 
        for element in meta_info:
            field = element["field"]
            data_type = element["type"]

            #Check if the data type corresponds to the required one if not change it accordingly. 
            if not isinstance(df.schema[field].dataType, data_type):
                if data_type == TimestampType:
                    df = df.withColumn(field, to_timestamp(col(field), element["format"]))
                else:
                    df = df.withColumn(field, col(field).cast(types_dict[data_type]))

        return df
    
    except Exception as e:
        LOGGER.exception(f"Data Validation failed: {e}")