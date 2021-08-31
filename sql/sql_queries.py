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
