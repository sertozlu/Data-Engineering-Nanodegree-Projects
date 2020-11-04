
# Data Modeling with Apache Cassandra

## **Overview**
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analysis team is particularly interested in understanding what songs users are listening to. Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app.

In this project, I applied data modeling with Apache Cassandra by creating tables in to run queries. I used an ETL pipeline using Python (partially provided) that transfers data from a set of CSV files within a directory to create a streamlined CSV file to model and insert data into Apache Cassandra tables.

## Dataset
For this project, there is only one dataset: event_data. The directory of CSV files partitioned by date. Here are examples of filepaths to two files in the dataset:
```
event_data/2018-11-08-events.csv
event_data/2018-11-09-events.csv
```

## Project Files

```event_data``` -> Directory that contains CSV files by date.

```event_datafile_new.csv``` -> Combination of all CSV files in event_data directory.

```images/image_event_datafile_new.jpg``` -> screenshot of how denormalized data appears in the event_datafile_new.csv.  

```Project_1B_Project_Template.ipynb``` ->  Jupyter notebook file that processes the event_datafile_new.csv dataset to create a denormalized dataset, models the data tables, loads the data into Apache Cassandra tables and runs the queries. 

## How to run

Evironment should have Python with cassandra package installed. 

Open and run Jupyter notebook ```Project_1B_Project_Template.ipynb``` 

