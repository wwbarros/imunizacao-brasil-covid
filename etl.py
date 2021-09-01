# Bibliotecas utilizadas
import configparser
import pandas as pd
from pyspark import sql
from sql import sql_queries
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
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

def check_has_content(df: DataFrame) -> bool:
    r = df.first()
    return r is not None

def check_uniquekey(df, columns_list):
    return df.groupBy(*columns_list).count().filter('count > 1').count() == 0

def create_spark(app_name: str) -> SparkSession:
    # Spark session
    spark = SparkSession.builder.appName(app_name).getOrCreate()

    return spark

def get_data_table(spark: SparkSession, df: DataFrame, table_name: str, sql_select: str) -> DataFrame:
    df.createOrReplaceTempView(table_name)
    sql_df = spark.sql(sql_select.format(table_name))

    return sql_df

def extract(spark: SparkSession, config_values: dict) -> DataFrame:
    df = spark.read.csv(config_values['source_data'], sep=';', header=True)

    return df

def transform(df: DataFrame, config_values: dict) -> DataFrame:
    # Obter a lista de colunas que serão utilizadas
    col_names = pd.read_json(config_values['columns'], typ='series')
    colunas_utilizadas = col_names.index

    # Obter as colunas não utilizadas
    colunas_naoutilizadas = list(set(df.columns) - set(colunas_utilizadas))
    df = df.drop(*colunas_naoutilizadas)

    # Trocar os valores Nulos
    df = df.fillna(\
    {\
        'vacina_categoria_codigo': 0, \
        'vacina_categoria_nome': 'N/A', \
        'vacina_grupoatendimento_nome': '',\
        'paciente_enumsexobiologico': 'N/A',\
        'paciente_endereco_nmmunicipio': 'N/A', \
        'paciente_endereco_nmpais': 'N/A', \
        'paciente_endereco_uf': 'N/A', \
        'estalecimento_nofantasia': 'N/A'
    })

    return df

def load(spark: SparkSession, df: DataFrame):
    # Criar tabela dimensão Vacinas
    vacinas = get_data_table(spark, df, "vacinas", sql_queries.vacinas_select)
    vacinas.printSchema()

    # Criar tabela dimensão Estabelecimentos
    estabelecimentos = get_data_table(spark, df, "estabelecimentos", sql_queries.estabelecimentos_select)
    estabelecimentos.printSchema()

    # Criar tabela dimensão Categorias
    categorias = get_data_table(spark, df, "categorias", sql_queries.categorias_select)
    categorias.printSchema()

    # Criar tabela dimensão Grupos de Atendimento
    grupos = get_data_table(spark, df, "grupos", sql_queries.grupos_select)
    grupos.printSchema()

    # Criar tabela fato Vacinação
    vacinacao = get_data_table(spark, df, "vacinacao", sql_queries.vacinacao_select)
    vacinacao.printSchema()
    # Check data quality
    if not check_has_content(vacinacao): raise Exception('No content: Vacinacao')
    if not check_nulls(vacinacao, \
        ['paciente_id', \
        'estabelecimento', \
        'categoria', \
        'grupoatendimento', \
        'vacina', \
        'idade', \
        'sexo', \
        'uf', \
        'municipio', \
        'lote', \
        'dose', \
        'dataaplicacao'], 0): raise Exception('Null: Vacinacao')

def main():
    # Read config file
    config = configparser.ConfigParser()
    config.read_file(open('dl.cfg'))
    
    DATA_LOCATION = config['COMMON']['DATA_LOCATION']
    config_values = {
        'columns': config['COMMON']['DATA_COLUMNS'],
        'source_data': config[DATA_LOCATION]['INPUT_DATA_VACCINES'],
        'output_data': config[DATA_LOCATION]['OUTPUT_DATA']
    }

    spark = create_spark('Brasil - Programa de imunização - COVID-19')
    
    df = extract(spark, config_values)

    df = transform(df, config_values)

    load(spark, df)

if __name__ == "__main__":
    main()