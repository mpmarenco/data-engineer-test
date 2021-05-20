# Data Engineer take home assingment

This application processes the file dataset.json lacated in the *data/* folder of the current project to a couple of derivatives and calculates a metric

## Table of Contents

- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Test](#test)
- [Application](#application)
- [Run with jobs](#Run with jobs)

## Prerequisites

* [Git](http://git-scm.com/)
* [Python3](https://www.python.org/downloads/)
* [Java JDK 8 +](https://www.java.com/es/download/) 

### Installation

* `git clone ` this repository
* `cd data-engineer-test`

You will need to install the requirements from the requirements.txt file. 
Follow one of the next options:

```bash
python3 -m pip install -r requirements.txt
pip intall -r requirements.txt
```

### Test

In order to run the tests the requirements should be intalled. 
Run the following line to run the tests

```bash
pytest test/unit.py
```

### Run Application

The application support two types of jobs 

1. job = load_events: 
Select the events *app_loaded* and *registered* and save them in the home directory as partitioned parquet files:

```bash
Ex:
/home/<user>/events/app_loaded/
/home/<user>/events/registered/
```
for Linux

```bash
Ex:
C:\Users\<user>\events\app_loaded\
C:\Users\<user>\events\registered\
```
for windows

The events files follow the event specification. Which means that only the fields defined by the
specification are taken into account. Fields such as channel and device_type can be nullable since 
there are events from *registered* that dont have channel and events from *app_loaded* that dont have 
device_type

Even though during data exploration all the fields except form "time" had the especified data type
the application validate the data againts the event specification.

```bash
{
    "registere" : {
        "time" : "timestamp",
        "initiator_id" : "long",
        "channel" : "string"
    },
    "app_loaded" : {
        "time" : "timestamp",
        "initiator_id" : "long",
        "device_type" : "string"
    }
}
```

2. Job = calculate_metric

Calculate the fraction of users who loaded the application at leat once during the
calendar week after registration. The calendar week does not include the date of
registration. 

if an user is registered on 2020-01-01 (Wednesday) then it will only be considered to the metric
if it loaded the application during the perior of time of next Monday until next Sunday (2020-01-06 - 2020-01-12)

The output will be display in the console as follows and the percentage will be rounded to
2 decimal places.

```bash
                 //-------------------------------------------------------------------
                //-------------------------------------------------------------------

                Fraction of users who loaded the app at least once during the calendar 
                week after the registration: 7.57 %

                //--------------------------------------------------------------------
                //-------------------------------------------------------------------
```

### Run with jobs

```bash
python main.py --job=load_events    # For first option
```
```bash
python main.py --job=calculate_metric    # For second option
```

### Disclamer

This application only makes use of pyspark it does not use any distributed file system such as HDFS.
