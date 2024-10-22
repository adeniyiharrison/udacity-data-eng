# Project 5: Data Pipelines with Airflow

### Introduction
A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

They have decided to bring you into the project and expect you to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

## Project Overview
This project will introduce you to the core concepts of Apache Airflow. To complete the project, you will need to create your own custom operators to perform tasks such as staging the data, filling the data warehouse, and running checks on the data as the final step.

We have provided you with a project template that takes care of all the imports and provides four empty operators that need to be implemented into functional pieces of a data pipeline. The template also contains a set of tasks that need to be linked to achieve a coherent and sensible data flow within the pipeline.

You'll be provided with a helpers class that contains all the SQL transformations. Thus, you won't need to write the ETL yourselves, but you'll need to execute it with your custom operators.

![Example_DAG](img/example-dag.png)

## File/Directory Structure

__DAG and Query Definition__
* `udac_example_dag.py` is where the DAG is created and the tasks are definted/linked together
* `create_tables.sql` creates the tables in the Redshift DB
* `sql_queries.py` are the queries used to migrate data from staging tables to fact/dimension tables

__Operator Definition__
* `stage_redshift.py` defines `StageToRedshiftOperator`, copies data from s3 to Redshift DB
* `load_dimension.py` defines `LoadDimensionOperator`, loads data to dim table from stage.
* `load_fact.py` defines `LoadFactOperator`, loads data to fact table from stage.
* `data_quality.py` defines `DataQualityOperator`, checks whether data in Redshift is as expected

## How to Run
* Download Airlow Image // run container
    * http://localhost:8080/admin/
    * https://towardsdatascience.com/getting-started-with-airflow-using-docker-cd8b44dbff98
* Create Redshift Cluster
    * Setup VPC security group to allow inbound access to port 5439
* Setup Airflow connections
    * AWS credentials
    * Connection to Postgres database