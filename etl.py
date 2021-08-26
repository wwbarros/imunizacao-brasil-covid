# Import necessary libraries
import pandas as pd
import uuid
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
import configparser

# Read config file
config = configparser.ConfigParser()
config.read_file(open('dl.cfg'))

INPUT_DATA = config['LOCAL']['INPUT_DATA']
INPUT_DATA_VACCINES = config['LOCAL']['INPUT_DATA_VACCINES']
OUTPUT_DATA = config['LOCAL']['OUTPUT_DATA']
DATA_COLUMNS = config['COMMON']['DATA_COLUMNS']

@udf(StringType())
def null_id_to_uuid (id):
    if id == None or id == "null":
        return str(uuid.uuid4().hex)
    return id
    
def write_parquet(pandas_df, parquet_name):
    parquet_path = OUTPUT_DATA + f'{parquet_name}.parquet'
    df = spark.createDataFrame(pandas_df)
    df.write.mode("overwrite").parquet(parquet_path)
    print(f'Writing {parquet_name} Table DONE.')

# Spark session
spark = SparkSession \
        .builder\
        .appName("Brazilian COVID-19 Immunization")\
        .getOrCreate()

vaccines_df = pd.read_csv(INPUT_DATA_VACCINES, sep=';', header=0, nrows=50)
col_names = pd.read_json(DATA_COLUMNS, typ='series')
valid_columns = col_names.index
columns_todrop = list(set(vaccines_df.columns) - set(valid_columns))
vaccines_df = vaccines_df.drop(columns=columns_todrop)
vaccines_df = vaccines_df.fillna(\
    {\
        'vacina_categoria_codigo': 0, \
        'vacina_categoria_nome': 'N/A', \
        'vacina_grupoatendimento_nome': 'N/A', \
        'paciente_enumsexobiologico': 'N/A',\
        'paciente_endereco_nmmunicipio': 'N/A', \
        'paciente_endereco_nmpais': 'N/A', \
        'paciente_endereco_uf': 'N/A', \
        'estalecimento_nofantasia': 'N/A'
    })
# Write staging table and read back
write_parquet(vaccines_df, 'staging_immunization')

