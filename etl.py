# Bibliotecas utilizadas
import configparser
import pandas as pd
from sql import sql_queries
from pyspark.sql import SparkSession
import pyspark.sql.types as T

def write_parquet(df, file_path, parquet_name, mode):
    parquet_path = f'{file_path}{parquet_name}.parquet'
    df.write.mode(mode).parquet(parquet_path)

def read_parquet(spark, file_path, parquet_name):
    parquet_path = f'{file_path}{parquet_name}.parquet'
    return spark.read.parquet(parquet_path)

def check_nulls(spark, df, columns_list, expected_value):
    df.createOrReplaceTempView("viewcheck")
    sql_check = f"SELECT COUNT(*) FROM viewcheck WHERE 1 <> 1 {''.join([' OR ' + c + ' IS NULL ' for c in columns_list])}"
    
    dfcheck = spark.sql(sql_check)
    
    value_check = dfcheck.collect()[0][0]
    
    return value_check == expected_value

def check_has_content(df):
    return df.count() > 0

def check_uniquekey(df, columns_list):
    return df.groupBy(*columns_list).count().filter('count > 1').count() == 0

def create_spark(app_name):
    # Spark session
    spark = SparkSession.builder.appName(app_name).getOrCreate()

    return spark

def extract(spark, path_dados_csv):
    df = spark.read.csv(path_dados_csv, sep=';', header=True)

    return df

def transform(df):
    print('transform')

def load():
    print('load')

def main():
    # Read config file
    config = configparser.ConfigParser()
    config.read_file(open('dl.cfg'))
    
    DATA_COLUMNS = config['COMMON']['DATA_COLUMNS']
    DATA_LOCATION = config['COMMON']['DATA_LOCATION']
    INPUT_DATA = config[DATA_LOCATION]['INPUT_DATA']
    INPUT_DATA_VACCINES = config[DATA_LOCATION]['INPUT_DATA_VACCINES']
    OUTPUT_DATA = config[DATA_LOCATION]['OUTPUT_DATA']

    spark = create_spark('Brasil - Programa de imunização - COVID-19')
    
    df = extract(spark, INPUT_DATA_VACCINES)

    transform(df)

    load()

if __name__ == "__main__":
    main()