
# Data Pipelines

## **Overview**
A music streaming company has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and decide to use Apache Airflow to achieve this.

In this project, I built a dynamic Airflow data pipeline from reusable tasks that can be monitored and allow easy backfills. In order to ensure data quality, I put checks to identify any discrepancies between original datasets and datasets after the ETL. I created custom operators to perform tasks such as staging the data, filling the data warehouse, and running checks on the data as the final step.

## Datasets
For this project, there are two datasets that reside in S3: 

Song data: ```s3://udacity-dend/song_data```

Log data: ```s3://udacity-dend/log_data```

Data needs to be processed in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

## Project Files

```dags/udac_example_dag.py``` -> Has all the imports and task templates with task dependencies set.

```create_tables.sql``` -> SQL table creation statements

```plugins/helpers/sql_queries.py``` -> SQL transformations

```plugins/operators/*.py``` -> Operator templates to create tables in Redshift, stage data from S3 to Redshift, load fact and dimension tables and check data quality.

## How to run

Add Airflow connections i.e. use Airflow's UI to configure your AWS credentials and connection to Redshift.

Run the dag from Airflow UI.