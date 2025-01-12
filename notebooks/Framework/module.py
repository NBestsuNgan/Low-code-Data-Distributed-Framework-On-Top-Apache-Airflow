from types import ModuleType
import json
import boto3
from botocore.config import Config
import pandas as pd
from io import StringIO, BytesIO
from pyspark.sql import SparkSession
import pyarrow.parquet as pq
from datetime import datetime
import oracledb


class Utility(ModuleType):
    @staticmethod
    def register_catalog():
        s3 = boto3.client(
            's3',
            endpoint_url='http://minio:9000',  # Replace with your MinIO server URL
            config=Config(signature_version='s3v4')
        )
        return s3

    @staticmethod
    def register_oracle():
        connection = oracledb.connect(
            user="sys",
            password="mypassword1",
            dsn="oracle19:1521/orclpdb1",
            mode=oracledb.SYSDBA
        )        
        return connection

    def af_registry_schema(bucket_name, path_to_file):
        s3 = Utility.register_catalog()
        response = s3.get_object(Bucket=bucket_name, Key=path_to_file)
        json_data = json.loads(response['Body'].read().decode('utf-8'))

        return json_data


    def af_read_csv(bucket_name, path_to_file, schema_registry):
        s3 = Utility.register_catalog()
        response = s3.get_object(Bucket=bucket_name, Key=path_to_file)
        csv_data = response['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(csv_data), on_bad_lines='skip')
        return df

    def af_read_from_staging(path_to_file):
        s3 = Utility.register_catalog()
        spark = SparkSession.builder \
            .appName("Read Parquet from S3") \
            .getOrCreate()
        
        response = s3.list_objects_v2(Bucket='warehouse', Prefix=path_to_file+'/data/')        
        df_list = []        
        for obj in response.get('Contents', []):
            object_key = obj['Key']
            
            if object_key.endswith('.parquet'):
                response = s3.get_object(Bucket='warehouse', Key=object_key)
                parquet_data = response['Body'].read()
                table = pq.read_table(BytesIO(parquet_data))
                df_spark = spark.createDataFrame(table.to_pandas())  # Convert pandas DataFrame to Spark DataFrame
                df_list.append(df_spark)
        
        full_df_spark = df_list[0]
        for df in df_list[1:]:
            full_df_spark = full_df_spark.union(df)
        
        return full_df_spark


    def af_write_to_staging(table_path, data):
        spark = SparkSession.builder \
            .appName("TestPySpark") \
            .getOrCreate()
        
        schema = spark.table(table_path.split('/')[0] + '.' + table_path.split('/')[1]).schema
        df = spark.createDataFrame(data, schema)
        df.writeTo(table_path.split('/')[0] + '.' + table_path.split('/')[1]).append()
        print("Success write to staging")

    def af_write_to_oracle(table_name, df):
        data_to_insert = df.collect()  
        columns = df.columns  
        
        connection = Utility.register_oracle()  
        oc = connection.cursor()
        
        placeholders = ", ".join([f":{i+1}" for i in range(len(columns))])
        insert_sql = f"INSERT INTO EDP.{table_name} ({', '.join(columns)}) VALUES ({placeholders})"
    
        data_as_tuples = [tuple(row) for row in data_to_insert]
        
        try:
            oc.executemany(insert_sql, data_as_tuples)
            connection.commit()
            print("Data inserted successfully!")
            oc.close()
            connection.close()
        except Exception as e:
            raise(f"Error occurred: {e}")

    def af_execute_to_oracle(bucket_name, script_path):
        connection = Utility.register_oracle()  
        oc = connection.cursor()

        s3 = Utility.register_catalog()
        response = s3.get_object(Bucket=bucket_name, Key=script_path)
        script = response['Body'].read().decode('utf-8')
        try:
            statements = script.strip().split(';')
            for stmt in statements:
                if stmt.strip():  # Skip empty statements
                    print(f"Executing statement:\n{stmt.strip()}\n")
                    oc.execute(stmt.strip())
            
            # Commit changes to the database
            connection.commit()

            oc.close()
            connection.close()
            print("Execution Successful")
        except Exception as e:
            print(f"Error executing script: {e}")





