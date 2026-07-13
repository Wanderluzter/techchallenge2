# Databricks notebook source
# DBTITLE 1,Implementacao em Memoria
# MAGIC %md
# MAGIC # Pipeline Híbrida — Análise da Alfabetização no Brasil
# MAGIC **Tech Challenge Fase 2 | Databricks | Azure**
# MAGIC
# MAGIC Pipeline integrada com **BigQuery** via `basedosdados.read_sql()`. Extrai dados do projeto `basedosdados.br_inep_avaliacao_alfabetizacao` com decodificação via JOIN ao `dicionario` e enriquecimento com `br_bd_diretorios_brasil` diretamente no BigQuery. Aplica transformações Silver/Gold em Spark e valida qualidade.
# MAGIC
# MAGIC ## Fontes de dados — `basedosdados.br_inep_avaliacao_alfabetizacao` (BigQuery)
# MAGIC
# MAGIC | Tabela | Grain | Modo | Decodificação |
# MAGIC | --- | --- | --- | --- |
# MAGIC | `uf` | ano, sigla_uf, serie, rede | Batch | JOIN dicionario + diretorio_uf |
# MAGIC | `municipio` | ano, id_municipio, serie, rede | Batch | JOIN dicionario + diretorio_municipio |
# MAGIC | `alunos` | ano, id_aluno | Batch + Streaming | JOIN dicionario (5 colunas) |
# MAGIC | `meta_alfabetizacao_brasil` | ano, rede | Batch + Streaming | Já decodificada na fonte |
# MAGIC | `meta_alfabetizacao_uf` | ano, sigla_uf, rede | Batch + Streaming | Já decodificada na fonte |
# MAGIC | `meta_alfabetizacao_municipio` | ano, id_municipio, rede | Batch + Streaming | Já decodificada na fonte |
# MAGIC
# MAGIC Colunas categóricas (`serie`, `rede`, `presenca`, `preenchimento_caderno`, `alfabetizado`) são decodificadas via JOIN com `dicionario` na query SQL de extração (dados chegam já como texto).
# MAGIC
# MAGIC ## Estrutura do notebook
# MAGIC
# MAGIC | Célula | Conteúdo |
# MAGIC | --- | --- |
# MAGIC | **Instalação de Dependências** | `%pip install basedosdados` |
# MAGIC | **Credenciais GCP** | Configuração `~/.basedosdados/` para autenticação BigQuery |
# MAGIC | **Setup e Extração Bronze** | `bd.read_sql()` com CTEs de dicionário + LEFT JOIN diretórios |
# MAGIC | **Transformações Silver** | Dedup, validação de integridade referencial e consistência |
# MAGIC | **Produtos Gold** | 5 datasets analíticos: indicador, gap meta, evolução temporal, perfil proficiência, painel executivo |
# MAGIC | **Validação de Qualidade** | 30 checks: unicidade, completude, consistência, integridade, positividade |
# MAGIC | **Resultados Finais** | Display dos produtos Gold |
# MAGIC
# MAGIC > Documentação completa: `README.md` e `modelo_solucao.md` na mesma pasta.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Instalacao de Dependencias
# MAGIC %pip install basedosdados --quiet

# COMMAND ----------

# DBTITLE 1,Credenciais GCP
# =============================================================================
# CREDENCIAIS GCP — configuração para basedosdados v2.0.3
# =============================================================================
# O basedosdados espera:
#   ~/.basedosdados/config.toml     (configuração geral)
#   ~/.basedosdados/credentials/prod.json    (service account key)
#   ~/.basedosdados/credentials/staging.json (pode ser cópia do prod)
#
# Para testes: faça upload dos JSONs para a pasta tech_challenge/
#              e adicione ao .gitignore
# =============================================================================
import pathlib, shutil

BD_DIR = pathlib.Path.home() / ".basedosdados"
CREDENTIALS_DIR = BD_DIR / "credentials"
CREDENTIALS_DIR.mkdir(parents=True, exist_ok=True)

# --- 1. Copiar credenciais do workspace para o path esperado ---
PROJECT_DIR = "/Workspace/Users/leobotelho27@outlook.com/techchallenge2"

for cred_file in ["prod.json", "staging.json"]:
    src = pathlib.Path(PROJECT_DIR) / cred_file
    dst = CREDENTIALS_DIR / cred_file
    if src.exists():
        shutil.copy(src, dst)
        print(f"✅ {cred_file} copiado → {dst}")
    else:
        print(f"⚠️  {cred_file} não encontrado em {PROJECT_DIR}")

# Se só tem prod.json, copia como staging.json também
prod_path = CREDENTIALS_DIR / "prod.json"
staging_path = CREDENTIALS_DIR / "staging.json"
if prod_path.exists() and not staging_path.exists():
    shutil.copy(prod_path, staging_path)
    print(f"✅ staging.json criado como cópia de prod.json")

# --- 2. Criar config.toml no formato exato da v2.0.3 ---
config_toml = BD_DIR / "config.toml"
config_toml.write_text(
    f'# basedosdados config (gerado automaticamente)\n'
    f'bucket_name = "basedosdados-latlong-481312"\n\n'
    f'[gcloud-projects]\n\n'
    f'    [gcloud-projects.staging]\n'
    f'    name = "latlong-481312"\n'
    f'    credentials_path = "{staging_path}"\n\n'
    f'    [gcloud-projects.prod]\n'
    f'    name = "latlong-481312"\n'
    f'    credentials_path = "{prod_path}"\n\n'
    f'[api]\n'
    f'url = "https://api.basedosdados.org/api/v1/graphql"\n'
)
print(f"✅ config.toml criado em {config_toml}")
print(f"   Conteúdo:")
print(config_toml.read_text())

# COMMAND ----------

# DBTITLE 1,Setup e Fixtures Bronze
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)
from typing import Dict, List, Tuple
import datetime
import basedosdados as bd
import pandas as pd

# =============================================================================
# CONFIGURAÇÃO BigQuery — Base dos Dados
# =============================================================================

BILLING_PROJECT_ID = "latlong-481312"
DATASET_ID = "br_inep_avaliacao_alfabetizacao"

# LIMITE para testes (remover/None em produção)
LIMIT_ROWS = 10000

# =============================================================================
# EXTRAÇÃO BRONZE — BigQuery via basedosdados (com JOINs ao dicionário)
# =============================================================================

def extract_sql_from_bq(query: str, billing_project_id: str, from_file: bool = True) -> DataFrame:
    """Executa SQL no BigQuery e converte para Spark DataFrame."""
    pdf = bd.read_sql(query=query, billing_project_id=billing_project_id, from_file=from_file)
    return spark.createDataFrame(pdf)


def extract_table_from_bq(dataset_id: str, table_id: str, billing_project_id: str, limit: int = None, from_file: bool = True) -> DataFrame:
    """Extrai tabela simples (sem decodificação) do BigQuery."""
    print(f"  Extraindo {dataset_id}.{table_id} ...")
    pdf = bd.read_table(dataset_id=dataset_id, table_id=table_id, billing_project_id=billing_project_id, limit=limit, from_file=from_file)
    return spark.createDataFrame(pdf)


# --- Queries com JOIN ao dicionário (dados já vem decodificados) ---

limit_clause = f"LIMIT {LIMIT_ROWS}" if LIMIT_ROWS else ""

QUERY_UF = f"""
WITH
  dicionario_serie AS (
    SELECT chave AS chave_serie, valor AS serie
    FROM `basedosdados.{DATASET_ID}.dicionario`
    WHERE nome_coluna = 'serie' AND id_tabela = 'uf'
  ),
  dicionario_rede AS (
    SELECT chave AS chave_rede, valor AS rede
    FROM `basedosdados.{DATASET_ID}.dicionario`
    WHERE nome_coluna = 'rede' AND id_tabela = 'uf'
  )
SELECT
    dados.ano,
    dados.sigla_uf,
    diretorio_uf.nome AS nome_uf,
    dicionario_serie.serie,
    dicionario_rede.rede,
    dados.taxa_alfabetizacao,
    dados.media_portugues,
    dados.proporcao_aluno_nivel_0,
    dados.proporcao_aluno_nivel_1,
    dados.proporcao_aluno_nivel_2,
    dados.proporcao_aluno_nivel_3,
    dados.proporcao_aluno_nivel_4,
    dados.proporcao_aluno_nivel_5,
    dados.proporcao_aluno_nivel_6,
    dados.proporcao_aluno_nivel_7,
    dados.proporcao_aluno_nivel_8
FROM `basedosdados.{DATASET_ID}.uf` AS dados
LEFT JOIN (SELECT DISTINCT sigla, nome FROM `basedosdados.br_bd_diretorios_brasil.uf`) AS diretorio_uf
    ON dados.sigla_uf = diretorio_uf.sigla
LEFT JOIN dicionario_serie ON dados.serie = chave_serie
LEFT JOIN dicionario_rede ON dados.rede = chave_rede
{limit_clause}
"""

QUERY_MUNICIPIO = f"""
WITH
  dicionario_serie AS (
    SELECT chave AS chave_serie, valor AS serie
    FROM `basedosdados.{DATASET_ID}.dicionario`
    WHERE nome_coluna = 'serie' AND id_tabela = 'municipio'
  ),
  dicionario_rede AS (
    SELECT chave AS chave_rede, valor AS rede
    FROM `basedosdados.{DATASET_ID}.dicionario`
    WHERE nome_coluna = 'rede' AND id_tabela = 'municipio'
  )
SELECT
    dados.ano,
    dados.id_municipio,
    diretorio_mun.nome AS nome_municipio,
    dicionario_serie.serie,
    dicionario_rede.rede,
    dados.taxa_alfabetizacao,
    dados.media_portugues,
    dados.proporcao_aluno_nivel_0,
    dados.proporcao_aluno_nivel_1,
    dados.proporcao_aluno_nivel_2,
    dados.proporcao_aluno_nivel_3,
    dados.proporcao_aluno_nivel_4,
    dados.proporcao_aluno_nivel_5,
    dados.proporcao_aluno_nivel_6,
    dados.proporcao_aluno_nivel_7,
    dados.proporcao_aluno_nivel_8
FROM `basedosdados.{DATASET_ID}.municipio` AS dados
LEFT JOIN (SELECT DISTINCT id_municipio AS mun_id, nome FROM `basedosdados.br_bd_diretorios_brasil.municipio`) AS diretorio_mun
    ON dados.id_municipio = diretorio_mun.mun_id
LEFT JOIN dicionario_serie ON dados.serie = chave_serie
LEFT JOIN dicionario_rede ON dados.rede = chave_rede
{limit_clause}
"""

QUERY_ALUNOS = f"""
WITH
  dicionario_serie AS (
    SELECT chave AS chave_serie, valor AS serie
    FROM `basedosdados.{DATASET_ID}.dicionario`
    WHERE nome_coluna = 'serie' AND id_tabela = 'alunos'
  ),
  dicionario_rede AS (
    SELECT chave AS chave_rede, valor AS rede
    FROM `basedosdados.{DATASET_ID}.dicionario`
    WHERE nome_coluna = 'rede' AND id_tabela = 'alunos'
  ),
  dicionario_presenca AS (
    SELECT chave AS chave_presenca, valor AS presenca
    FROM `basedosdados.{DATASET_ID}.dicionario`
    WHERE nome_coluna = 'presenca' AND id_tabela = 'alunos'
  ),
  dicionario_preenchimento AS (
    SELECT chave AS chave_preenchimento, valor AS preenchimento_caderno
    FROM `basedosdados.{DATASET_ID}.dicionario`
    WHERE nome_coluna = 'preenchimento_caderno' AND id_tabela = 'alunos'
  ),
  dicionario_alfabetizado AS (
    SELECT chave AS chave_alfabetizado, valor AS alfabetizado
    FROM `basedosdados.{DATASET_ID}.dicionario`
    WHERE nome_coluna = 'alfabetizado' AND id_tabela = 'alunos'
  )
SELECT
    dados.ano,
    dados.id_municipio,
    dados.id_escola,
    dados.id_aluno,
    dados.caderno,
    dicionario_serie.serie,
    dicionario_rede.rede,
    dicionario_presenca.presenca,
    dicionario_preenchimento.preenchimento_caderno,
    dicionario_alfabetizado.alfabetizado,
    dados.proficiencia,
    dados.peso_aluno
FROM `basedosdados.{DATASET_ID}.alunos` AS dados
LEFT JOIN dicionario_serie ON dados.serie = chave_serie
LEFT JOIN dicionario_rede ON dados.rede = chave_rede
LEFT JOIN dicionario_presenca ON dados.presenca = chave_presenca
LEFT JOIN dicionario_preenchimento ON dados.preenchimento_caderno = chave_preenchimento
LEFT JOIN dicionario_alfabetizado ON dados.alfabetizado = chave_alfabetizado
{limit_clause}
"""

# --- Executar extrações ---
print(f"=== EXTRAINDO TABELAS DO BIGQUERY (limit={LIMIT_ROWS}) ===")

print("  UF (com dicionário + diretório) ...")
bronze_uf_df = extract_sql_from_bq(QUERY_UF, BILLING_PROJECT_ID)

print("  Município (com dicionário + diretório) ...")
bronze_municipio_df = extract_sql_from_bq(QUERY_MUNICIPIO, BILLING_PROJECT_ID)

print("  Alunos (com dicionário) ...")
bronze_alunos_df = extract_sql_from_bq(QUERY_ALUNOS, BILLING_PROJECT_ID)

# Metas já vem com rede decodificada da fonte — extração simples
print("  Metas (extração direta) ...")
bronze_meta_brasil_df = extract_table_from_bq(
    dataset_id=DATASET_ID, table_id="meta_alfabetizacao_brasil",
    billing_project_id=BILLING_PROJECT_ID, limit=LIMIT_ROWS,
)
bronze_meta_uf_df = extract_table_from_bq(
    dataset_id=DATASET_ID, table_id="meta_alfabetizacao_uf",
    billing_project_id=BILLING_PROJECT_ID, limit=LIMIT_ROWS,
)
bronze_meta_municipio_df = extract_table_from_bq(
    dataset_id=DATASET_ID, table_id="meta_alfabetizacao_municipio",
    billing_project_id=BILLING_PROJECT_ID, limit=LIMIT_ROWS,
)

# Consolidar
BRONZE_TABLES: Dict[str, DataFrame] = {
    "bronze_uf": bronze_uf_df,
    "bronze_municipio": bronze_municipio_df,
    "bronze_alunos": bronze_alunos_df,
    "bronze_meta_brasil": bronze_meta_brasil_df,
    "bronze_meta_uf": bronze_meta_uf_df,
    "bronze_meta_municipio": bronze_meta_municipio_df,
}

print("\n=== BRONZE TABLES LOADED (dados já decodificados via dicionário BQ) ===")
for name, df in BRONZE_TABLES.items():
    print(f"  {name}: {df.count()} rows")


# COMMAND ----------

# DBTITLE 1,Transformacoes Silver
# =============================================================================
# TRANSFORMAÇÕES SILVER
# (Dados já vem decodificados do BigQuery via JOIN ao dicionário na extração)
# =============================================================================

def transform_silver_dim_uf(bronze_uf: DataFrame, bronze_meta_uf: DataFrame) -> DataFrame:
    """Cria dimensão de UF normalizada."""
    uf_from_indicadores = bronze_uf.select("sigla_uf").distinct()
    uf_from_metas = bronze_meta_uf.select("sigla_uf").distinct()
    
    return (
        uf_from_indicadores.union(uf_from_metas)
        .filter(F.col("sigla_uf").isNotNull())
        .dropDuplicates(["sigla_uf"])
        .orderBy("sigla_uf")
    )


def transform_silver_dim_municipio(bronze_mun: DataFrame, bronze_meta_mun: DataFrame, bronze_alunos: DataFrame = None) -> DataFrame:
    """Cria dimensão de município normalizada (inclui alunos para integridade referencial)."""
    mun_from_indicadores = bronze_mun.select("id_municipio").distinct()
    mun_from_metas = bronze_meta_mun.select("id_municipio").distinct()
    
    all_mun = mun_from_indicadores.union(mun_from_metas)
    
    if bronze_alunos is not None:
        mun_from_alunos = bronze_alunos.select("id_municipio").distinct()
        all_mun = all_mun.union(mun_from_alunos)
    
    return (
        all_mun
        .filter(F.col("id_municipio").isNotNull())
        .dropDuplicates(["id_municipio"])
        .orderBy("id_municipio")
    )


def transform_silver_indicadores_uf(bronze_uf: DataFrame) -> DataFrame:
    """Transforma indicadores de UF: valida e deduplica (dados já decodificados)."""
    df = bronze_uf.filter(F.col("sigla_uf").isNotNull())
    
    # Calcula soma das proporções para validação (ignora linhas all-null)
    proporcao_cols = [f"proporcao_aluno_nivel_{i}" for i in range(9)]
    df = df.withColumn(
        "tem_proporcoes",
        F.coalesce(*[F.col(c) for c in proporcao_cols]).isNotNull()
    ).withColumn(
        "soma_proporcoes",
        F.when(F.col("tem_proporcoes"),
               sum(F.coalesce(F.col(c), F.lit(0.0)) for c in proporcao_cols)
        ).otherwise(F.lit(None))
    )
    
    return df.dropDuplicates(["ano", "sigla_uf", "serie", "rede"])


def transform_silver_indicadores_municipio(bronze_mun: DataFrame, dim_municipio: DataFrame) -> DataFrame:
    """Transforma indicadores de município (dados já decodificados)."""
    df = bronze_mun.filter(F.col("id_municipio").isNotNull())
    
    # Calcula soma das proporções (ignora linhas all-null)
    proporcao_cols = [f"proporcao_aluno_nivel_{i}" for i in range(9)]
    df = df.withColumn(
        "tem_proporcoes",
        F.coalesce(*[F.col(c) for c in proporcao_cols]).isNotNull()
    ).withColumn(
        "soma_proporcoes",
        F.when(F.col("tem_proporcoes"),
               sum(F.coalesce(F.col(c), F.lit(0.0)) for c in proporcao_cols)
        ).otherwise(F.lit(None))
    )
    
    # Valida integridade referencial com dimensão
    df = df.join(
        dim_municipio.select(F.col("id_municipio").alias("dim_id_municipio")),
        df.id_municipio == F.col("dim_id_municipio"),
        "left"
    ).withColumn(
        "municipio_valido",
        F.col("dim_id_municipio").isNotNull()
    ).drop("dim_id_municipio")
    
    return df.dropDuplicates(["ano", "id_municipio", "serie", "rede"])


def transform_silver_microdados_alunos(bronze_alunos: DataFrame, dim_municipio: DataFrame) -> DataFrame:
    """Transforma microdados de alunos: deduplica e valida (dados já decodificados)."""
    df = bronze_alunos.filter(F.col("id_aluno").isNotNull())
    
    # Valida integridade referencial
    df = df.join(
        dim_municipio.select(F.col("id_municipio").alias("dim_id_municipio")),
        df.id_municipio == F.col("dim_id_municipio"),
        "left"
    ).withColumn(
        "municipio_valido",
        F.col("dim_id_municipio").isNotNull()
    ).drop("dim_id_municipio")
    
    return df.dropDuplicates(["ano", "id_aluno"])


def transform_silver_meta_brasil(bronze_meta: DataFrame) -> DataFrame:
    """Transforma metas nacionais."""
    return (
        bronze_meta
        .filter(F.col("ano").isNotNull() & F.col("rede").isNotNull())
        .dropDuplicates(["ano", "rede"])
        .withColumn(
            "meta_valida",
            F.col("meta_alfabetizacao_2030") >= F.col("meta_alfabetizacao_2024")
        )
    )


def transform_silver_meta_uf(bronze_meta_uf: DataFrame, dim_uf: DataFrame) -> DataFrame:
    """Transforma metas por UF."""
    df = bronze_meta_uf.filter(F.col("sigla_uf").isNotNull())
    
    df = df.join(
        dim_uf.select(F.col("sigla_uf").alias("dim_sigla_uf")),
        df.sigla_uf == F.col("dim_sigla_uf"),
        "left"
    ).withColumn(
        "uf_valida",
        F.col("dim_sigla_uf").isNotNull()
    ).drop("dim_sigla_uf")
    
    return df.dropDuplicates(["ano", "sigla_uf", "rede"])


def transform_silver_meta_municipio(bronze_meta_mun: DataFrame, dim_municipio: DataFrame) -> DataFrame:
    """Transforma metas por município."""
    df = bronze_meta_mun.filter(F.col("id_municipio").isNotNull())
    
    df = df.join(
        dim_municipio.select(F.col("id_municipio").alias("dim_id_municipio")),
        df.id_municipio == F.col("dim_id_municipio"),
        "left"
    ).withColumn(
        "municipio_valido",
        F.col("dim_id_municipio").isNotNull()
    ).drop("dim_id_municipio")
    
    return df.dropDuplicates(["ano", "id_municipio", "rede"])


# --- Executar transformações Silver ---
silver_dim_uf = transform_silver_dim_uf(bronze_uf_df, bronze_meta_uf_df)
silver_dim_municipio = transform_silver_dim_municipio(bronze_municipio_df, bronze_meta_municipio_df, bronze_alunos_df)
silver_indicadores_uf = transform_silver_indicadores_uf(bronze_uf_df)
silver_indicadores_municipio = transform_silver_indicadores_municipio(bronze_municipio_df, silver_dim_municipio)
silver_microdados_alunos = transform_silver_microdados_alunos(bronze_alunos_df, silver_dim_municipio)
silver_meta_brasil = transform_silver_meta_brasil(bronze_meta_brasil_df)
silver_meta_uf = transform_silver_meta_uf(bronze_meta_uf_df, silver_dim_uf)
silver_meta_municipio = transform_silver_meta_municipio(bronze_meta_municipio_df, silver_dim_municipio)

SILVER_TABLES: Dict[str, DataFrame] = {
    "silver_dim_uf": silver_dim_uf,
    "silver_dim_municipio": silver_dim_municipio,
    "silver_indicadores_uf": silver_indicadores_uf,
    "silver_indicadores_municipio": silver_indicadores_municipio,
    "silver_microdados_alunos": silver_microdados_alunos,
    "silver_meta_brasil": silver_meta_brasil,
    "silver_meta_uf": silver_meta_uf,
    "silver_meta_municipio": silver_meta_municipio,
}

print("\n=== SILVER TABLES TRANSFORMED ===")
for name, df in SILVER_TABLES.items():
    print(f"  {name}: {df.count()} rows")


# COMMAND ----------

# DBTITLE 1,Produtos Gold
# =============================================================================
# PRODUTOS GOLD
# =============================================================================

def build_gold_indicador_municipio(
    silver_indicadores_mun: DataFrame,
    silver_dim_mun: DataFrame
) -> DataFrame:
    """Gold: Indicador consolidado por município para dashboards regionais."""
    # Seleciona apenas id_municipio da dimensão (nome já vem do indicador)
    return (
        silver_indicadores_mun
        .select(
            "ano", "id_municipio",
            "serie", "rede",
            "taxa_alfabetizacao", "media_portugues",
            "proporcao_aluno_nivel_0", "proporcao_aluno_nivel_1",
            "proporcao_aluno_nivel_2", "proporcao_aluno_nivel_3",
            "proporcao_aluno_nivel_4", "proporcao_aluno_nivel_5",
            "proporcao_aluno_nivel_6", "proporcao_aluno_nivel_7",
            "proporcao_aluno_nivel_8",
            "soma_proporcoes", "municipio_valido"
        )
    )


def build_gold_gap_meta_resultado(
    silver_indicadores_mun: DataFrame,
    silver_meta_mun: DataFrame
) -> DataFrame:
    """Gold: Gap entre meta planejada e resultado observado por município.
    
    Ambas as tabelas (indicadores e metas) já vem com `rede` decodificada
    do BigQuery, permitindo JOIN direto sem conversão.
    """
    indicadores = silver_indicadores_mun.select(
        "ano", "id_municipio", "rede",
        F.col("taxa_alfabetizacao").alias("taxa_realizada")
    )
    
    metas = silver_meta_mun.select(
        "ano", "id_municipio", "rede",
        "meta_alfabetizacao_2024", "meta_alfabetizacao_2025",
        "meta_alfabetizacao_2026", "meta_alfabetizacao_2027",
        "meta_alfabetizacao_2028", "meta_alfabetizacao_2029",
        "meta_alfabetizacao_2030",
        "nivel_alfabetizacao", "percentual_participacao"
    )
    
    joined = indicadores.join(
        metas,
        ["ano", "id_municipio", "rede"],
        "left"
    )
    
    # Calcular gap em relação à meta 2024 (ano vigente próximo)
    return joined.withColumn(
        "gap_meta_2024",
        F.col("meta_alfabetizacao_2024") - F.col("taxa_realizada")
    ).withColumn(
        "gap_meta_2030",
        F.col("meta_alfabetizacao_2030") - F.col("taxa_realizada")
    ).withColumn(
        "acima_meta_2024",
        F.col("taxa_realizada") >= F.col("meta_alfabetizacao_2024")
    )


def build_gold_evolucao_temporal(
    silver_indicadores_uf: DataFrame
) -> DataFrame:
    """Gold: Série histórica para análise de tendência por UF."""
    return (
        silver_indicadores_uf
        .select(
            "ano", "sigla_uf",
            "serie", "rede",
            "taxa_alfabetizacao", "media_portugues"
        )
        .orderBy("ano", "sigla_uf", "rede")
    )


def build_gold_perfil_proficiencia(
    silver_alunos: DataFrame,
    silver_dim_mun: DataFrame
) -> DataFrame:
    """Gold: Perfil de proficiência agregado por município."""
    # Filtrar apenas alunos presentes com proficiência válida
    alunos_presentes = silver_alunos.filter(
        (F.col("presenca") == "Presente") & F.col("proficiencia").isNotNull()
    )
    
    perfil = (
        alunos_presentes
        .groupBy("ano", "id_municipio", "rede", "serie")
        .agg(
            F.count("*").alias("total_alunos"),
            F.avg("proficiencia").alias("proficiencia_media"),
            F.expr("percentile_approx(proficiencia, 0.5)").alias("proficiencia_mediana"),
            F.min("proficiencia").alias("proficiencia_min"),
            F.max("proficiencia").alias("proficiencia_max"),
            F.sum(F.when(F.col("alfabetizado") == "Alfabetizado", 1).otherwise(0)).alias("qtd_alfabetizados"),
            F.sum(F.when(F.col("proficiencia") >= 743, 1).otherwise(0)).alias("qtd_acima_corte_743")
        )
        .withColumn(
            "percentual_alfabetizado",
            F.round(F.col("qtd_alfabetizados") / F.col("total_alunos") * 100, 2)
        )
        .withColumn(
            "percentual_acima_corte",
            F.round(F.col("qtd_acima_corte_743") / F.col("total_alunos") * 100, 2)
        )
    )
    
    # Join com dimensão usando alias para evitar ambiguidade
    dim_renamed = silver_dim_mun.withColumnRenamed("id_municipio", "dim_id")
    return perfil.join(dim_renamed, perfil.id_municipio == dim_renamed.dim_id, "left").drop("dim_id")


def build_gold_painel_executivo(
    gold_indicador_mun: DataFrame,
    gold_gap: DataFrame,
    silver_meta_brasil: DataFrame
) -> DataFrame:
    """Gold: Visão executiva nacional agregada por ano e rede."""
    # Agregar indicadores por rede
    indicadores_nacionais = (
        gold_indicador_mun
        .groupBy("ano", "rede")
        .agg(
            F.count("*").alias("total_municipios"),
            F.avg("taxa_alfabetizacao").alias("taxa_media_nacional"),
            F.min("taxa_alfabetizacao").alias("taxa_minima"),
            F.max("taxa_alfabetizacao").alias("taxa_maxima")
        )
    )
    
    # Contar municípios acima da meta
    municipios_acima_meta = (
        gold_gap
        .filter(F.col("acima_meta_2024") == True)
        .groupBy("ano", "rede")
        .agg(F.count("*").alias("municipios_acima_meta"))
    )
    
    # Juntar com meta nacional
    return (
        indicadores_nacionais
        .join(municipios_acima_meta, ["ano", "rede"], "left")
        .join(
            silver_meta_brasil.select(
                "ano", "rede",
                F.col("meta_alfabetizacao_2024").alias("meta_2024"),
                F.col("meta_alfabetizacao_2030").alias("meta_2030")
            ),
            ["ano", "rede"],
            "left"
        )
        .withColumn(
            "percentual_municipios_acima_meta",
            F.round(F.col("municipios_acima_meta") / F.col("total_municipios") * 100, 2)
        )
        .withColumn(
            "gap_nacional",
            F.col("meta_2024") - F.col("taxa_media_nacional")
        )
    )


# --- Construir produtos Gold ---
gold_indicador_municipio = build_gold_indicador_municipio(silver_indicadores_municipio, silver_dim_municipio)
gold_gap_meta_resultado = build_gold_gap_meta_resultado(silver_indicadores_municipio, silver_meta_municipio)
gold_evolucao_temporal = build_gold_evolucao_temporal(silver_indicadores_uf)
gold_perfil_proficiencia = build_gold_perfil_proficiencia(silver_microdados_alunos, silver_dim_municipio)
gold_painel_executivo = build_gold_painel_executivo(gold_indicador_municipio, gold_gap_meta_resultado, silver_meta_brasil)

GOLD_TABLES: Dict[str, DataFrame] = {
    "gold_indicador_municipio": gold_indicador_municipio,
    "gold_gap_meta_resultado": gold_gap_meta_resultado,
    "gold_evolucao_temporal": gold_evolucao_temporal,
    "gold_perfil_proficiencia": gold_perfil_proficiencia,
    "gold_painel_executivo": gold_painel_executivo,
}

print("\n=== GOLD TABLES BUILT ===")
for name, df in GOLD_TABLES.items():
    print(f"  {name}: {df.count()} rows")


# COMMAND ----------

# DBTITLE 1,Validacao de Qualidade
# =============================================================================
# VALIDAÇÃO DE QUALIDADE
# =============================================================================

from dataclasses import dataclass
from typing import Optional

@dataclass
class QualityCheckResult:
    check_name: str
    table_name: str
    passed: bool
    total_rows: int
    failed_rows: int
    message: str


def check_uniqueness(df: DataFrame, table_name: str, grain_cols: List[str]) -> QualityCheckResult:
    """Verifica unicidade por grain."""
    total = df.count()
    distinct = df.select(grain_cols).distinct().count()
    duplicates = total - distinct
    passed = duplicates == 0
    return QualityCheckResult(
        check_name="Unicidade",
        table_name=table_name,
        passed=passed,
        total_rows=total,
        failed_rows=duplicates,
        message=f"Grain: {grain_cols} | Duplicatas: {duplicates}"
    )


def check_not_null(df: DataFrame, table_name: str, col_name: str) -> QualityCheckResult:
    """Verifica completude (não nulo) de uma coluna."""
    total = df.count()
    nulls = df.filter(F.col(col_name).isNull()).count()
    passed = nulls == 0
    return QualityCheckResult(
        check_name="Completude",
        table_name=table_name,
        passed=passed,
        total_rows=total,
        failed_rows=nulls,
        message=f"Coluna: {col_name} | Nulos: {nulls}"
    )


def check_range(df: DataFrame, table_name: str, col_name: str, min_val: float, max_val: float) -> QualityCheckResult:
    """Verifica se valores estão dentro de uma faixa."""
    total = df.filter(F.col(col_name).isNotNull()).count()
    out_of_range = df.filter(
        (F.col(col_name) < min_val) | (F.col(col_name) > max_val)
    ).count()
    passed = out_of_range == 0
    return QualityCheckResult(
        check_name="Consistência (faixa)",
        table_name=table_name,
        passed=passed,
        total_rows=total,
        failed_rows=out_of_range,
        message=f"Coluna: {col_name} | Fora de [{min_val}, {max_val}]: {out_of_range}"
    )


def check_referential_integrity(df: DataFrame, table_name: str, flag_col: str) -> QualityCheckResult:
    """Verifica integridade referencial usando coluna de flag."""
    total = df.count()
    invalid = df.filter(F.col(flag_col) == False).count()
    passed = invalid == 0
    return QualityCheckResult(
        check_name="Integridade Referencial",
        table_name=table_name,
        passed=passed,
        total_rows=total,
        failed_rows=invalid,
        message=f"Flag: {flag_col} | Inválidos: {invalid}"
    )


def check_proportion_sum(df: DataFrame, table_name: str, tolerance: float = 5.0) -> QualityCheckResult:
    """Verifica se soma das proporções está próxima de 100 (ignora linhas sem dados de proporção)."""
    # Filtra apenas linhas que TEM proporções (soma_proporcoes != NULL)
    df_com_proporcoes = df.filter(F.col("soma_proporcoes").isNotNull())
    total = df_com_proporcoes.count()
    
    if total == 0:
        return QualityCheckResult(
            check_name="Consistência (soma proporções)",
            table_name=table_name,
            passed=True,
            total_rows=0,
            failed_rows=0,
            message=f"Sem dados de proporção (all NULL) — check ignorado"
        )
    
    out_of_tolerance = df_com_proporcoes.filter(
        (F.col("soma_proporcoes") < 100 - tolerance) | (F.col("soma_proporcoes") > 100 + tolerance)
    ).count()
    passed = out_of_tolerance == 0
    return QualityCheckResult(
        check_name="Consistência (soma proporções)",
        table_name=table_name,
        passed=passed,
        total_rows=total,
        failed_rows=out_of_tolerance,
        message=f"Avaliadas: {total} | Fora de tolerância ({tolerance}%): {out_of_tolerance}"
    )


def check_monotonic_metas(df: DataFrame, table_name: str) -> QualityCheckResult:
    """Verifica se metas são monotonicamente crescentes (2024 <= ... <= 2030)."""
    total = df.count()
    non_monotonic = df.filter(
        F.col("meta_alfabetizacao_2030") < F.col("meta_alfabetizacao_2024")
    ).count()
    passed = non_monotonic == 0
    return QualityCheckResult(
        check_name="Consistência (trajetória metas)",
        table_name=table_name,
        passed=passed,
        total_rows=total,
        failed_rows=non_monotonic,
        message=f"Metas não crescentes (2024→2030): {non_monotonic}"
    )


def check_positive(df: DataFrame, table_name: str, col_name: str) -> QualityCheckResult:
    """Verifica se valores não-nulos são positivos (> 0)."""
    df_non_null = df.filter(F.col(col_name).isNotNull())
    total = df_non_null.count()
    negatives = df_non_null.filter(F.col(col_name) <= 0).count()
    passed = negatives == 0
    return QualityCheckResult(
        check_name="Consistência (positivo)",
        table_name=table_name,
        passed=passed,
        total_rows=total,
        failed_rows=negatives,
        message=f"Coluna: {col_name} | Não-positivos: {negatives}"
    )


# --- Executar todas as validações (conforme README Seção 7) ---
quality_results: List[QualityCheckResult] = []

# ===================== UNICIDADE =====================
# Bronze
quality_results.append(check_uniqueness(bronze_uf_df, "bronze_uf", ["ano", "sigla_uf", "serie", "rede"]))
quality_results.append(check_uniqueness(bronze_municipio_df, "bronze_municipio", ["ano", "id_municipio", "serie", "rede"]))
quality_results.append(check_uniqueness(bronze_alunos_df, "bronze_alunos", ["ano", "id_aluno"]))
quality_results.append(check_uniqueness(bronze_meta_brasil_df, "bronze_meta_brasil", ["ano", "rede"]))
quality_results.append(check_uniqueness(bronze_meta_uf_df, "bronze_meta_uf", ["ano", "sigla_uf", "rede"]))
quality_results.append(check_uniqueness(bronze_meta_municipio_df, "bronze_meta_municipio", ["ano", "id_municipio", "rede"]))
# Silver
quality_results.append(check_uniqueness(silver_indicadores_uf, "silver_indicadores_uf", ["ano", "sigla_uf", "serie", "rede"]))
quality_results.append(check_uniqueness(silver_indicadores_municipio, "silver_indicadores_municipio", ["ano", "id_municipio", "serie", "rede"]))
quality_results.append(check_uniqueness(silver_microdados_alunos, "silver_microdados_alunos", ["ano", "id_aluno"]))

# ===================== COMPLETUDE =====================
quality_results.append(check_not_null(bronze_uf_df, "bronze_uf", "ano"))
quality_results.append(check_not_null(bronze_uf_df, "bronze_uf", "sigla_uf"))
quality_results.append(check_not_null(bronze_municipio_df, "bronze_municipio", "ano"))
quality_results.append(check_not_null(bronze_municipio_df, "bronze_municipio", "id_municipio"))
quality_results.append(check_not_null(bronze_alunos_df, "bronze_alunos", "ano"))
quality_results.append(check_not_null(bronze_alunos_df, "bronze_alunos", "id_aluno"))
quality_results.append(check_not_null(silver_indicadores_uf, "silver_indicadores_uf", "taxa_alfabetizacao"))
quality_results.append(check_not_null(silver_indicadores_municipio, "silver_indicadores_municipio", "taxa_alfabetizacao"))

# ===================== CONSISTÊNCIA =====================
# Faixa taxa_alfabetizacao [0, 100]
quality_results.append(check_range(silver_indicadores_uf, "silver_indicadores_uf", "taxa_alfabetizacao", 0, 100))
quality_results.append(check_range(silver_indicadores_municipio, "silver_indicadores_municipio", "taxa_alfabetizacao", 0, 100))
# Soma proporções ≈ 100 (tolerância 1% conforme README)
quality_results.append(check_proportion_sum(silver_indicadores_uf, "silver_indicadores_uf", tolerance=1.0))
quality_results.append(check_proportion_sum(silver_indicadores_municipio, "silver_indicadores_municipio", tolerance=1.0))
# Proficiência e peso_aluno positivos
quality_results.append(check_positive(silver_microdados_alunos, "silver_microdados_alunos", "proficiencia"))
quality_results.append(check_positive(silver_microdados_alunos, "silver_microdados_alunos", "peso_aluno"))
# Metas monotônicas (2024 ≤ ... ≤ 2030)
quality_results.append(check_monotonic_metas(silver_meta_brasil, "silver_meta_brasil"))
quality_results.append(check_monotonic_metas(silver_meta_uf, "silver_meta_uf"))
quality_results.append(check_monotonic_metas(silver_meta_municipio, "silver_meta_municipio"))

# ===================== INTEGRIDADE REFERENCIAL =====================
quality_results.append(check_referential_integrity(silver_indicadores_municipio, "silver_indicadores_municipio", "municipio_valido"))
quality_results.append(check_referential_integrity(silver_microdados_alunos, "silver_microdados_alunos", "municipio_valido"))
quality_results.append(check_referential_integrity(silver_meta_uf, "silver_meta_uf", "uf_valida"))
quality_results.append(check_referential_integrity(silver_meta_municipio, "silver_meta_municipio", "municipio_valido"))

# --- Gerar relatório ---
print("\n" + "="*80)
print("RELATÓRIO DE QUALIDADE DE DADOS")
print("="*80)

passed_count = sum(1 for r in quality_results if r.passed)
failed_count = len(quality_results) - passed_count

print(f"\nTotal de verificações: {len(quality_results)}")
print(f"✅ Passou: {passed_count}")
print(f"❌ Falhou: {failed_count}")
print("\n" + "-"*80)

for r in quality_results:
    status = "✅" if r.passed else "❌"
    print(f"{status} [{r.table_name}] {r.check_name}: {r.message}")


# COMMAND ----------

# DBTITLE 1,Resultados Finais
# =============================================================================
# VISUALIZAÇÃO DOS RESULTADOS FINAIS
# =============================================================================

print("\n" + "="*80)
print("AMOSTRA DOS PRODUTOS GOLD")
print("="*80)

print("\n--- gold_indicador_municipio ---")
display(gold_indicador_municipio)

print("\n--- gold_gap_meta_resultado ---")
display(gold_gap_meta_resultado)

print("\n--- gold_evolucao_temporal ---")
display(gold_evolucao_temporal)

print("\n--- gold_perfil_proficiencia ---")
display(gold_perfil_proficiencia)

print("\n--- gold_painel_executivo ---")
display(gold_painel_executivo)

print("\n" + "="*80)
print("PIPELINE COMPLETO EXECUTADO COM SUCESSO (em memória)")
print("="*80)
print(f"Bronze: {len(BRONZE_TABLES)} tabelas")
print(f"Silver: {len(SILVER_TABLES)} tabelas")
print(f"Gold: {len(GOLD_TABLES)} produtos analíticos")
print(f"Validações: {len(quality_results)} checks executados")
