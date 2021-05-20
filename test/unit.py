import os
from datetime import datetime
from subprocess import call
from click.testing import CliRunner
from pyspark.sql import SparkSession
import shutil

from src.load_events import dataValidation
from unittest import mock, TestCase
from parameterized import parameterized
from pyspark.sql.types import *

from src.load_events import process as load_events_process
from src.calculate_metric import process as calculate_metric_process

from pandas._testing import assert_frame_equal


def are_dfs_equal(df1, df2):
    return df1.schema == df2.schema and df1.count() == df2.count()

events = {
    "registered" : ["time", "initiator_id", "channel"],
    "app_loaded" : ["time", "initiator_id", "device_type"]
}
meta_info = [
    {
        "field" : "time",
        "type": TimestampType,
        "format": "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
    },
    {
        "field": "initiator_id",
        "type": LongType
    }
]


class TestBacktesting(TestCase):

    # Test files that had the required event especification and test validate function with different input data schema
    input_values_load_event = [
        ("test/data/input/data_test.json", "test/data/output/events"),
        ("test/data/input/data_val_test.json", "test/data/output/events_val")
    ]

    @parameterized.expand(input_values_load_event)
    def test_load_events(self, input_path, output_path_expected):

        spark = SparkSession.builder.appName("test-std").getOrCreate()
        spark.catalog.clearCache()
        output_path = "test/data/temp/"
        load_events_process(input_path, output_path, events, meta_info)

        df_test_registered = spark.read.parquet("test/data/temp/registered")
        df_test_registered_extpected = spark.read.parquet(f"{output_path_expected}/registered")

        assert are_dfs_equal(df_test_registered, df_test_registered_extpected)

        df_test_loaded = spark.read.parquet("test/data/temp/app_loaded")
        df_test_loaded_extpected = spark.read.parquet(f"{output_path_expected}/app_loaded")

        assert are_dfs_equal(df_test_loaded, df_test_loaded_extpected)
        
        shutil.rmtree("test/data/temp")
        

    def test_calculate_metric(self):

        spark = SparkSession.builder.appName("test-std").getOrCreate()
        spark.catalog.clearCache()

        val = calculate_metric_process("test/data/input/registered", "test/data/input/app_loaded")

        assert (val == "33.33")
      