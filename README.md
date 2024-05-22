# Telecom Customer Churn - A Data Engineering and Data Analysis project
This repository contains my project on a End to end data science project on a Telecom Dataset. The project includes the EDA notebook and a Tableau Story for a Stakeholder Report.
Project highlights:
1. Tableau Story - Stakeholder Report
2. EDA notebook
3. custom-elt-project

# Tableau Story - Stakeholder Report
Available at link - https://public.tableau.com/shared/NBWRHKZ33?:display_count=n&:origin=viz_share_link

The 7 page story details:
- Dataset Intro Statistics
- Correlation Map for feature trimming
- General Linear Model for feature selection
- Customer Demographics vs Churn
- Account Features vs Churn
- Opt-In Services vs Churn
- Q4 Revenue vs Churn

<img width="1131" alt="image" src="https://github.com/Gugan0905/Telecom_Customer_Churn_Tableau_Data_Analysis/assets/43416760/6638f90f-5791-44de-b69d-4b75dc2d5f56">

# EDA Notebook

The EDA consists of the following sections:
- Data Preprocessing
  -   Data type conversion
  -   Missing Value Handling
  -   Class Imbalance and Distribution
  -   Analyzing Dataset Feature
    -  _Customer Demographics_
    -  _Location Data_
    -  _Opt-In Services_
    -  _Payment Features_
  -   Distribution of Continous Features
- Feature Selection
  - Correlation Mapping
  - General Linear Modelling
- Recursive Feature Elimination
- Modelling _(Logistic Regression, SVM, Random Forests, Decision Trees, Naive Bayes)_
- Model Optimization
  - Hyper-parameter Tuning - Randomized Search Cross Validation

# Dataset Used
[IBM Fictional Telecom Dataset](https://accelerator.ca.analytics.ibm.com/bi/?perspective=authoring&pathRef=.public_folders%2FIBM%2BAccelerator%2BCatalog%2FContent%2FDAT00148&id=i9710CF25EF75468D95FFFC7D57D45204&objRef=i9710CF25EF75468D95FFFC7D57D45204&action=run&format=HTML&cmPropStr=%7B%22id%22%3A%22i9710CF25EF75468D95FFFC7D57D45204%22%2C%22type%22%3A%22reportView%22%2C%22defaultName%22%3A%22DAT00148%22%2C%22permissions%22%3A%5B%22execute%22%2C%22read%22%2C%22traverse%22%5D%7D)


# Overview of the Custom ELT Project

Docker based ETL pipeline managing data transfer from src to dest. PostgreSQL RDB, with an integrated Flask UI for data entry. The Airflow/CRON job orchestrated pipeline periodically uploads to Amazon S3 / Redshift, for further EDA and Tableau visualization.
Proceed in to the *custom-elt-project folder* for more details

