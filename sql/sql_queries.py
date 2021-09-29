vacinas_select = ("""
SELECT DISTINCT CAST(vacina_codigo AS INT) AS codigo, 
                    vacina_nome AS descricao
    FROM {}
    ORDER BY codigo, descricao
""")

estabelecimentos_select = ("""
    SELECT DISTINCT CAST(estabelecimento_valor AS INT) AS codigo,
                    estalecimento_nofantasia AS descricao,
                    estabelecimento_razaosocial AS razaosocial,
                    estabelecimento_uf AS uf,
                    estabelecimento_municipio_nome AS municipio
    FROM {}
    ORDER BY uf, municipio, razaosocial, descricao
""")

categorias_select = ("""
    SELECT DISTINCT CAST(vacina_categoria_codigo AS INT) AS codigo,
                    vacina_categoria_nome AS descricao
            FROM {}
            ORDER BY codigo
""")

grupos_select = ("""
    SELECT DISTINCT CAST(vacina_grupoatendimento_codigo AS INT) AS codigo,
                    vacina_grupoatendimento_nome AS descricao
            FROM {}
        ORDER BY codigo
""")

vacinacao_select = ("""
    SELECT DISTINCT paciente_id AS paciente_id,
            CAST(estabelecimento_valor AS INT) AS estabelecimento,
            CAST(vacina_categoria_codigo AS INT) AS categoria,
            CAST(vacina_grupoatendimento_codigo AS INT) AS grupoatendimento,
            CAST(vacina_codigo AS INT) AS vacina, 
            CAST(paciente_idade AS INT) AS idade,
            paciente_enumsexobiologico AS sexo,
            estabelecimento_uf AS uf,
            estabelecimento_municipio_nome AS municipio,
            vacina_lote AS lote,
            vacina_fabricante_nome AS fornecedor,
            vacina_descricao_dose AS dose,                  
            vacina_dataaplicacao AS dataaplicacao
        FROM {} 
        WHERE paciente_id IS NOT NULL
        ORDER BY dataaplicacao
""")

select_queries = {
    'vacinas': vacinas_select, 
    'estabelecimentos': estabelecimentos_select,
    'categorias': categorias_select,
    'grupos': grupos_select,
    'vacinacao': vacinacao_select
}

default_upsert = ("""
INSERT dbo.{tabela}([codigo], [descricao]) 
  SELECT {codigo}, '{descricao}'
  WHERE NOT EXISTS
  (
    SELECT 1 FROM dbo.{tabela} WITH (UPDLOCK, SERIALIZABLE)
      WHERE [codigo] = {codigo}
  );
 
IF @@ROWCOUNT = 0
BEGIN
  UPDATE dbo.{tabela} SET [descricao] = '{descricao}' WHERE [codigo] = {codigo} AND '{descricao}' <> 'N/A';
END
""")

estabelecimentos_upsert = ("""
INSERT INTO [dbo].[estabelecimentos]
           ([codigo]
           ,[descricao]
           ,[razaosocial]
           ,[uf]
           ,[municipio])
     SELECT {codigo}
           ,'{descricao}'
           ,'{razaosocial}'
           ,'{uf}'
           ,'{municipio}'
       WHERE NOT EXISTS
      (
        SELECT 1 FROM dbo.[estabelecimentos] WITH (UPDLOCK, SERIALIZABLE)
          WHERE [codigo] = {codigo}
      );
IF @@ROWCOUNT = 0
BEGIN
  UPDATE [dbo].[estabelecimentos]
   SET [descricao] = '{descricao}'
      ,[razaosocial] = '{razaosocial}'
      ,[uf] = '{uf}'
      ,[municipio] = '{municipio}'
 WHERE [codigo] = {codigo} AND '{descricao}' <> 'N/A';
END
""")