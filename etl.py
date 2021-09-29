# Bibliotecas utilizadas
import configparser
import pandas as pd
import pyodbc
from sql import sql_queries
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


def create_db_connection(driver: str, server: str, database: str, user: str, password: str):
    conn_str = (f"DRIVER={driver};"
                f"Server={server};"
                f"Database={database};"
                f"UID={user};"
                f"PWD={password};")

    cnxn = pyodbc.connect(conn_str)

    return cnxn


def write_parquet(df: DataFrame, file_path: str, parquet_name: str, mode: str = "overwrite", partitionby: str = None):
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


def transform(spark: SparkSession, df: DataFrame, path_colunas_utilizadas: str, path_output: str):
    # Obter a lista de colunas que serão utilizadas
    col_names = pd.read_json(path_colunas_utilizadas, typ='series')
    colunas_utilizadas = col_names.index

    # Obter as colunas não utilizadas
    colunas_naoutilizadas = list(set(df.columns) - set(colunas_utilizadas))
    df = df.drop(*colunas_naoutilizadas)

    # Trocar os valores Nulos
    df = df.fillna(
        {
            'vacina_categoria_codigo': 0,
            'vacina_categoria_nome': 'N/A',
            'vacina_grupoatendimento_nome': 'N/A',
            'paciente_enumsexobiologico': 'N/A',
            'paciente_endereco_nmmunicipio': 'N/A',
            'paciente_endereco_nmpais': 'N/A',
            'paciente_endereco_uf': 'N/A',
            'estalecimento_nofantasia': 'N/A'
        })

    print('Criar tabela dimensão Vacinas')
    df_vacinas = get_data_table(
        spark, df, "vacinas", sql_queries.vacinas_select)
    save_dataframe_to_parquet(spark, df_vacinas, path_output, "vacinas", True, [
                              'codigo', 'descricao'], ['codigo'])

    print('Criar tabela dimensão Estabelecimentos')
    df_estabelecimentos = get_data_table(
        spark, df, "estabelecimentos", sql_queries.estabelecimentos_select)
    save_dataframe_to_parquet(spark, df_estabelecimentos, path_output,
                              "estabelecimentos", True, ['codigo', 'descricao'])

    print('Criar tabela dimensão Categorias')
    df_categorias = get_data_table(
        spark, df, "categorias", sql_queries.categorias_select)
    save_dataframe_to_parquet(
        spark, df_categorias, path_output, "categorias", True, ['codigo', 'descricao'])

    print('Criar tabela dimensão Grupos de Atendimento')
    df_grupos = get_data_table(spark, df, "grupos", sql_queries.grupos_select)
    save_dataframe_to_parquet(spark, df_grupos, path_output, "grupos", True, [
                              'codigo', 'descricao'])

    print('Criar tabela fato Vacinação')
    df_vacinacao = get_data_table(
        spark, df, "vacinacao", sql_queries.vacinacao_select)
    save_dataframe_to_parquet(spark, df_vacinacao, path_output, "vacinacao", True,
                              ['paciente_id', 'estabelecimento', 'categoria', 'grupoatendimento', 'vacina', 'idade', 'sexo', 'uf', 'municipio', 'lote', 'dose', 'dataaplicacao'])


def load(spark: SparkSession, path_output: str, db_connection: any):
    cursor = db_connection.cursor()

    df_vacinas = read_parquet(spark, path_output, "vacinas")
    for r in df_vacinas.collect():
        cursor.execute(sql_queries.default_upsert.format(tabela="vacinas", codigo=r.codigo, descricao=r.descricao))
    cursor.commit()
    
    df_categorias = read_parquet(spark, path_output, "categorias")
    for r in df_categorias.collect():
        cursor.execute(sql_queries.default_upsert.format(tabela="categorias", codigo=r.codigo, descricao=r.descricao))
    cursor.commit()

    df_grupos = read_parquet(spark, path_output, "grupos")
    for r in df_grupos.collect():
        cursor.execute(sql_queries.default_upsert.format(tabela="grupos", codigo=r.codigo, descricao=r.descricao))
    cursor.commit()

    df_estabelecimentos = read_parquet(spark, path_output, "estabelecimentos")
    for r in df_estabelecimentos.collect():
        cursor.execute(sql_queries.estabelecimentos_upsert.format(codigo=r.codigo, descricao=r.descricao.replace("'", ""), razaosocial=r.razaosocial.replace("'", ""), uf=r.uf, municipio=r.municipio.replace("'", "")))
    cursor.commit()


def save_dataframe_to_parquet(spark: SparkSession, df_table: DataFrame, file_path: str, table_name: str, check_content: bool = True, cols_notnull: list = None, cols_uniquekey: list = None):
    if check_content and not check_has_content(df_table):
        raise Exception(f'No content: {table_name}')
    if cols_notnull is not None and not check_nulls(spark, df_table, cols_notnull, 0):
        raise Exception(f'Null: {table_name}')
    if cols_uniquekey is not None and not check_uniquekey(df_table, cols_uniquekey):
        raise Exception(f'Unique Key Fail: {table_name}')

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

    transform(spark, df, config_values['columns'], config_values['output_data'])

    db = create_db_connection(
        config['DB']['DRIVER'],
        config['DB']['SERVER'],
        config['DB']['DATABASE'],
        config['DB']['USER'],
        config['DB']['PASSWORD'])

    load(spark, config_values['output_data'], db)


if __name__ == "__main__":
    main()
