CREATE TABLE [vacinas](
	[codigo] [int] NOT NULL,
	[descricao] [nvarchar](255) NOT NULL
)
CREATE TABLE [categorias](
	[codigo] [int] NOT NULL,
	[descricao] [nvarchar](255) NOT NULL
)

CREATE TABLE [estabelecimentos](
	[codigo] [int] NOT NULL,
	[descricao] [nvarchar](255) NOT NULL,
	[razaosocial] [nvarchar](255) NOT NULL,
	[uf] [nvarchar](2) NOT NULL,
	[municipio] [nvarchar](50) NOT NULL
)

CREATE TABLE [grupos](
	[codigo] [int] NOT NULL,
	[descricao] [nvarchar](255) NOT NULL
)

CREATE TABLE [vacinacao](
	[paciente_id] [nvarchar](100) NOT NULL,
	[estabelecimento] [int] NOT NULL,
	[categoria] [int] NOT NULL,
	[grupoatendimento] [int] NOT NULL,
	[vacina] [int] NOT NULL,
	[idade] [int] NOT NULL,
	[sexo] [nvarchar](2) NOT NULL,
	[uf] [nvarchar](2) NOT NULL,
	[municipio] [nvarchar](50) NOT NULL,
	[lote] [nvarchar](50) NOT NULL,
	[fornecedor] [nvarchar](50) NOT NULL,
	[dose] [nvarchar](50) NOT NULL,
	[dataaplicacao] [date] NOT NULL
);
