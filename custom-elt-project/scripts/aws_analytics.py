import boto3
import os
from io import StringIO
import pandas as pd
from sqlalchemy import create_engine

class awsAnalytics:
    def __init__(self, bucket_name, df=None, redshift_config=None, table_name=None):
        self.bucket_name = bucket_name
        self.df = df
        self.s3_key = 'telecom_customer_churn/cleaned_data.csv'
        self.redshift_config = redshift_config
        self.table_name = table_name

    def EDA_cleaning(self):
        # Concise steps taken from initial EDA notebook
        # Find full notebook at: https://github.com/Gugan0905/Telecom_Customer_Churn_Tableau_Data_Analysis/blob/main/Telecom_Customer_Churn_Analysis.ipynb
        cleaned_df = self.df.copy()
        # Ensuring Total Charges is the right data type
        cleaned_df['Total Charges'] = pd.to_numeric(cleaned_df['Total Charges'], errors='coerce')

        # Imputing Total Charges by median
        cleaned_df['Total Charges'] = cleaned_df['Total Charges'].fillna(cleaned_df['Total Charges'].median())

        # Replacing Nan in Churn Reason with Unknown
        cleaned_df['Churn Reason'] = cleaned_df['Churn Reason'].fillna('Unknown')

        return cleaned_df

    def upload_to_s3(self):
        if self.df is None:
            raise ValueError("DataFrame is not initialized")
        csv_buffer = StringIO()
        cleaned_df = self.EDA_cleaning()
        cleaned_df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        print("Uploading file to S3...")
        # Access credentials from environment variables
        aws_access_key_id = "your aws access key"
        aws_secret_access_key = "your aws secret key"

        # Create boto3 client with retrieved credentials
        s3_client = boto3.client('s3',
                                aws_access_key_id=aws_access_key_id,
                                aws_secret_access_key=aws_secret_access_key)        
        s3_client.put_object(Body=csv_buffer.getvalue(), Bucket=self.bucket_name, Key=self.s3_key)
        print("Upload to S3 complete.")

    def upload_to_redshift(self):
        if self.df is None:
            raise ValueError("DataFrame is not initialized")
        if self.redshift_config is None or self.table_name is None:
            raise ValueError("Redshift configuration or table name is not provided")

        cleaned_df = self.EDA_cleaning()
        engine = create_engine(
            f"postgresql+psycopg2://{self.redshift_config['user']}:{self.redshift_config['password']}@{self.redshift_config['host']}:{self.redshift_config['port']}/{self.redshift_config['dbname']}"
        )
        print("Uploading file to Redshift...")
        cleaned_df.to_sql(self.table_name, engine, index=False, if_exists='replace')
        print("Upload to Redshift complete.")
