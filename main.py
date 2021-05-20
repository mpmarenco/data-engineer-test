
from pathlib import Path
import click
from src.common import LOGGER
from src.load_events import process as load_events_process
from src.calculate_metric import process as calculate_metric_process
from pyspark.sql.types import *

jobs_process = ["load_events", "calculate_metric"]
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
    },
    {
        "field": "channel",
        "type": StringType
    },
    {
        "field": "device_type",
        "type": StringType
    }
]


@click.command()
@click.option(
    "--job",
    help="load_events, calculate_metric",
    required=True,
)
def main(job):

    if job not in jobs_process:
        raise ValueError(f"Job must be in ({', '.join(jobs_process)})")

    LOGGER.info(f"Job: {job}")

    try:

        output_path = str(Path.home()) + "/events/"
        LOGGER.info(f"Output path: {output_path}")

        if job == "load_events":
            input_path = "data/dataset.json"
            load_events_process(input_path, output_path, events, meta_info)
        
        if job == "calculate_metric":
            input_path_registered = output_path + "registered"
            input_path_loaded = output_path + "app_loaded"
            val = calculate_metric_process(input_path_registered, input_path_loaded)

            LOGGER.info(f"""

                //-------------------------------------------------------------------
                //-------------------------------------------------------------------

                Fraction of users who loaded the app at least once during the calendar 
                week after the registration: {val} %

                //--------------------------------------------------------------------
                //-------------------------------------------------------------------

            """)
            
    except Exception as e: 
        LOGGER.exception(f"Excecution status Failed: {e}")


if __name__ == "__main__":
    main(auto_envvar_prefix="X") 