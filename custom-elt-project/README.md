# Custom ELT Project

## Overview
Docker based ETL pipeline managing data transfer from src to dest. PostgreSQL RDB, with an integrated Flask UI for data entry. The Airflow orchestrated pipeline periodically uploads to Amazon S3 / Redshift, for further EDA and Tableau visualization.

## Folder Structure

- custom-elt-project
    - **dags** -> contains the script to perform periodical upload of the destination db to amazon s3 and redshift
    - **(Not required - Only Reference)** **dataset-to-sql** -> converts the static csv dataset in SQL ddl commands 
    - **elt_script** -> performs data transformation and source to target mapping
    - **flask_app** -> code to implement the UI for new data entry
    - **scripts** -> utility scripts to facilitate uploads to aws
    - **source_db_init** -> holds the result of the *dataset-to-sql/convert-csv-to-DDL.py* script


## Authentication requirements

Be sure to place your AWS keys in the docker compose file.
In addition, you would need to set up your s3 and redshift requirements and attach them in the file as well.

## Docker related files

This entire project operates in a docker environment. Be sure to have it installed.

In short, the docker compose file should take care of all the processing and setup needed for this project.

Once you have your docker installed and folder opened in terminal, execute


``` docker compose up init-airflow -d ```

Followed by

``` docker compose up ```


