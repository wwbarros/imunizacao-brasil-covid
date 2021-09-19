# Bibliotecas utilizadas
import configparser
import pandas as pd
from sql import sql_queries
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def write_parquet(df: DataFrame, file_path: str, parquet_name: str, mode: str="overwrite", partitionby: str=None):
    parquet_path = f'{file_path}{parquet_name}.parquet'
    df.write.mode(mode).parquet(parquet_path, partitionBy=partitionby)

def read_parquet(spark: SparkSession, file_path: str, parquet_name: str) -> DataFrame:
    parquet_path = f'{file_path}{parquet_name}.parquet'
    return spark.read.parquet(parquet_path)

def check_nulls(spark: SparkSession, df: DataFrame, columns_list: list, expected_value: any) -> bool:
    df.createOrReplaceTempView("viewcheck")
    sql_check = f"SELECT COUNT(*) FROM viewcheck WHERE 1 <> 1 {''.join([' OR ' + c + ' IS NULL ' for c in columns_list])}"
    
    dfcheck = spark.sql(sql_check)
    
    value_check = dfcheck.collect()[0][0]
    
    return value_check == expected_value

def check_has_content(df: DataFrame) -> bool:
    r = df.first()
    return r is not None

def check_uniquekey(df: DataFrame, columns_list: list) -> bool:
    return df.groupBy(*columns_list).count().filter('count > 1').count() == 0

def create_spark(app_name: str) -> SparkSession:
    print(f"Creating Spark session: {app_name}")
    spark = SparkSession.builder.appName(app_name).getOrCreate()

    return spark

def get_data_table(spark: SparkSession, df: DataFrame, table_name: str, sql_select: str) -> DataFrame:
    df.createOrReplaceTempView(table_name)
    sql_df = spark.sql(sql_select.format(table_name))

    return sql_df

def extract(spark: SparkSession, path_source: str) -> DataFrame:
    df = spark.read.csv(path_source, sep=';', header=True)

    return df

def transform(df: DataFrame, path_colunas_utilizadas: str) -> DataFrame:
    # Obter a lista de colunas que serão utilizadas
    col_names = pd.read_json(path_colunas_utilizadas, typ='series')
    colunas_utilizadas = col_names.index

    # Obter as colunas não utilizadas
    colunas_naoutilizadas = list(set(df.columns) - set(colunas_utilizadas))
    df = df.drop(*colunas_naoutilizadas)

    # Trocar os valores Nulos
    df = df.fillna(\
    {\
        'vacina_categoria_codigo': 0, \
        'vacina_categoria_nome': 'N/A', \
        'vacina_grupoatendimento_nome': 'N/A',\
        'paciente_enumsexobiologico': 'N/A',\
        'paciente_endereco_nmmunicipio': 'N/A', \
        'paciente_endereco_nmpais': 'N/A', \
        'paciente_endereco_uf': 'N/A', \
        'estalecimento_nofantasia': 'N/A'
    })

    return df

def load(spark: SparkSession, df: DataFrame, path_output: dict):
    print('Criar tabela dimensão Vacinas')
    save_dataframe_to_parquet(spark, df, path_output, "vacinas", sql_queries.vacinas_select, True, ['codigo', 'descricao'], ['codigo'])

    print('Criar tabela dimensão Estabelecimentos')
    save_dataframe_to_parquet(spark, df, path_output, "estabelecimentos", sql_queries.estabelecimentos_select, True, ['codigo', 'descricao'])

    print('Criar tabela dimensão Categorias')
    save_dataframe_to_parquet(spark, df, path_output, "categorias", sql_queries.categorias_select, True, ['codigo', 'descricao'])

    print('Criar tabela dimensão Grupos de Atendimento')
    save_dataframe_to_parquet(spark, df, path_output, "grupos", sql_queries.grupos_select, True, ['codigo', 'descricao'])

    print('Criar tabela fato Vacinação')
    save_dataframe_to_parquet(spark, df, path_output, "vacinacao", sql_queries.vacinacao_select, True, \
        ['paciente_id', 'estabelecimento', 'categoria', 'grupoatendimento', 'vacina', 'idade', 'sexo', 'uf', 'municipio', 'lote', 'dose', 'dataaplicacao'])
    

def save_dataframe_to_parquet(spark: SparkSession, df: DataFrame, file_path: str, table_name: str, sql_select: str, check_content: bool=True, cols_notnull: list=None, cols_uniquekey: list=None):
    df_table = get_data_table(spark, df, table_name, sql_select)

    if check_content and not check_has_content(df_table): raise Exception(f'No content: {table_name}')
    if cols_notnull is not None and not check_nulls(spark, df_table, cols_notnull, 0): raise Exception(f'Null: {table_name}')
    if cols_uniquekey is not None and not check_uniquekey(df_table, cols_uniquekey): raise Exception(f'Unique Key Fail: {table_name}')
    
    write_parquet(df_table, file_path, table_name)


def main():
    # Read config file
    config = configparser.ConfigParser()
    config.read_file(open('dl.cfg'))
    
    DATA_LOCATION = config['COMMON']['DATA_LOCATION']
    config_values = {
        'columns': config['COMMON']['DATA_COLUMNS'],
        'source_data': config[DATA_LOCATION]['INPUT_DATA'],
        'output_data': config[DATA_LOCATION]['OUTPUT_DATA']
    }

    spark = create_spark(config['COMMON']['APP_NAME'])
    
    df = extract(spark, config_values['source_data'])

    df = transform(df, config_values['columns'])

    load(spark, df, config_values['output_data'])


if __name__ == "__main__":
    main()