# Databricks notebook source
# DBTITLE 1,Visao Geral
# MAGIC %md
# MAGIC # Modelo de Solução - Tech Challenge Fase 2
# MAGIC
# MAGIC Este notebook foi estruturado como **modelo lógico e técnico** da solução, sem provisionar recursos em nuvem e sem criar tabelas, pipelines ou jobs.
# MAGIC
# MAGIC ## Objetivo
# MAGIC Construir a proposta de uma pipeline híbrida (batch + streaming) para análise da alfabetização no Brasil, seguindo as diretrizes do documento de referência e a arquitetura medalhão.
# MAGIC
# MAGIC ## Escopo coberto
# MAGIC * Contexto do problema e objetivos do projeto
# MAGIC * Arquitetura proposta para Azure/Databricks
# MAGIC * Modelo das camadas Bronze, Silver e Gold
# MAGIC * Regras de qualidade e governança
# MAGIC * Estratégia de monitoramento e FinOps
# MAGIC * Estrutura mínima de repositório e documentação
# MAGIC
# MAGIC ## Fontes previstas
# MAGIC * UF
# MAGIC * Meta Alfabetização Brasil
# MAGIC * Meta Alfabetização por UF
# MAGIC * Meta Alfabetização por Município
# MAGIC * Município
# MAGIC * Dados de alunos
# MAGIC
# MAGIC ## Premissas
# MAGIC * A implementação final usará Delta/Parquet como formato padrão
# MAGIC * A ingestão batch tratará dados históricos e cadastrais
# MAGIC * A ingestão streaming simulará atualizações quase em tempo real de indicadores e metas
# MAGIC * Este material serve como base para implementação posterior
# MAGIC

# COMMAND ----------

# DBTITLE 1,Arquitetura Proposta
# MAGIC %md
# MAGIC # Arquitetura proposta
# MAGIC
# MAGIC ## Desenho conceitual
# MAGIC 1. **Ingestão batch**: cargas periódicas de metas, cadastros territoriais e bases históricas.
# MAGIC 2. **Ingestão streaming**: eventos simulados de atualização de indicadores, resultados e metas.
# MAGIC 3. **Bronze**: dados brutos, históricos e auditáveis.
# MAGIC 4. **Silver**: padronização, limpeza, deduplicação, validação de chaves e integração entre entidades.
# MAGIC 5. **Gold**: datasets analíticos para dashboards, análises estatísticas e modelos preditivos.
# MAGIC
# MAGIC ## Diretrizes arquiteturais
# MAGIC * Separação clara entre ingestão, tratamento e consumo analítico
# MAGIC * Persistência em Delta Lake com versionamento e rastreabilidade
# MAGIC * Contratos de dados por camada
# MAGIC * Chaves normalizadas para integração entre município, UF, metas e indicadores
# MAGIC * Preparação da camada Gold para consumo por BI e IA
# MAGIC
# MAGIC ## Fluxo de dados esperado
# MAGIC **Fonte Base dos Dados / fontes externas opcionais -> Bronze -> Silver -> Gold -> Dashboards / Analytics / ML**
# MAGIC
# MAGIC ## Entregáveis desta modelagem
# MAGIC * Estrutura lógica do pipeline
# MAGIC * Funções-base de transformação
# MAGIC * Regras de qualidade
# MAGIC * Decisões de monitoramento, governança e custos
# MAGIC

# COMMAND ----------

# DBTITLE 1,Configuracao do Modelo
from dataclasses import dataclass, field
from typing import Dict, List

# MODELO DE CONFIGURAÇÃO
# Este notebook não cria recursos. Ele documenta e organiza a solução.
# Fonte: basedosdados.br_inep_avaliacao_alfabetizacao (BigQuery via Base dos Dados)

@dataclass
class SourceSchema:
    """Representa o contrato de colunas de uma fonte da Base dos Dados."""
    table_id: str
    bq_project: str
    bq_dataset: str
    bq_table: str
    grain: List[str]          # chave de unicidade natural
    key_columns: List[str]    # chaves de relacionamento com outras entidades
    measure_columns: List[str]
    dimension_columns: List[str]
    uses_dictionary: bool

@dataclass
class LayerConfig:
    name: str
    description: str
    expected_entities: List[str]

@dataclass
class SolutionModel:
    cloud: str
    runtime: str
    storage_format: str
    source_project: str
    batch_entities: List[str]
    streaming_simulated_entities: List[str]
    layers: Dict[str, LayerConfig] = field(default_factory=dict)


# Contratos de esquema por fonte
source_schemas: Dict[str, SourceSchema] = {

    "uf": SourceSchema(
        table_id="uf",
        bq_project="basedosdados",
        bq_dataset="br_inep_avaliacao_alfabetizacao",
        bq_table="uf",
        grain=["ano", "sigla_uf", "serie", "rede"],
        key_columns=["sigla_uf"],
        measure_columns=[
            "taxa_alfabetizacao", "media_portugues",
            "proporcao_aluno_nivel_0", "proporcao_aluno_nivel_1",
            "proporcao_aluno_nivel_2", "proporcao_aluno_nivel_3",
            "proporcao_aluno_nivel_4", "proporcao_aluno_nivel_5",
            "proporcao_aluno_nivel_6", "proporcao_aluno_nivel_7",
            "proporcao_aluno_nivel_8"
        ],
        dimension_columns=["ano", "sigla_uf", "sigla_uf_nome", "serie", "rede"],
        uses_dictionary=True
    ),

    "municipio": SourceSchema(
        table_id="municipio",
        bq_project="basedosdados",
        bq_dataset="br_inep_avaliacao_alfabetizacao",
        bq_table="municipio",
        grain=["ano", "id_municipio", "serie", "rede"],
        key_columns=["id_municipio"],
        measure_columns=[
            "taxa_alfabetizacao", "media_portugues",
            "proporcao_aluno_nivel_0", "proporcao_aluno_nivel_1",
            "proporcao_aluno_nivel_2", "proporcao_aluno_nivel_3",
            "proporcao_aluno_nivel_4", "proporcao_aluno_nivel_5",
            "proporcao_aluno_nivel_6", "proporcao_aluno_nivel_7",
            "proporcao_aluno_nivel_8"
        ],
        dimension_columns=["ano", "id_municipio", "id_municipio_nome", "serie", "rede"],
        uses_dictionary=True
    ),

    "alunos": SourceSchema(
        table_id="alunos",
        bq_project="basedosdados",
        bq_dataset="br_inep_avaliacao_alfabetizacao",
        bq_table="alunos",
        grain=["ano", "id_aluno"],
        key_columns=["id_municipio", "id_escola", "id_aluno"],
        measure_columns=["proficiencia", "peso_aluno"],
        dimension_columns=[
            "ano", "id_municipio", "id_municipio_nome", "id_escola", "id_aluno",
            "caderno", "serie", "rede", "presenca", "preenchimento_caderno", "alfabetizado"
        ],
        uses_dictionary=True
    ),

    "meta_alfabetizacao_brasil": SourceSchema(
        table_id="meta_alfabetizacao_brasil",
        bq_project="basedosdados",
        bq_dataset="br_inep_avaliacao_alfabetizacao",
        bq_table="meta_alfabetizacao_brasil",
        grain=["ano", "rede"],
        key_columns=[],
        measure_columns=[
            "taxa_alfabetizacao", "percentual_participacao",
            "meta_alfabetizacao_2024", "meta_alfabetizacao_2025",
            "meta_alfabetizacao_2026", "meta_alfabetizacao_2027",
            "meta_alfabetizacao_2028", "meta_alfabetizacao_2029",
            "meta_alfabetizacao_2030"
        ],
        dimension_columns=["ano", "rede"],
        uses_dictionary=False
    ),

    "meta_alfabetizacao_uf": SourceSchema(
        table_id="meta_alfabetizacao_uf",
        bq_project="basedosdados",
        bq_dataset="br_inep_avaliacao_alfabetizacao",
        bq_table="meta_alfabetizacao_uf",
        grain=["ano", "sigla_uf", "rede"],
        key_columns=["sigla_uf"],
        measure_columns=[
            "taxa_alfabetizacao", "percentual_participacao",
            "meta_alfabetizacao_2024", "meta_alfabetizacao_2025",
            "meta_alfabetizacao_2026", "meta_alfabetizacao_2027",
            "meta_alfabetizacao_2028", "meta_alfabetizacao_2029",
            "meta_alfabetizacao_2030"
        ],
        dimension_columns=["ano", "sigla_uf", "sigla_uf_nome", "rede"],
        uses_dictionary=False
    ),

    "meta_alfabetizacao_municipio": SourceSchema(
        table_id="meta_alfabetizacao_municipio",
        bq_project="basedosdados",
        bq_dataset="br_inep_avaliacao_alfabetizacao",
        bq_table="meta_alfabetizacao_municipio",
        grain=["ano", "id_municipio", "rede"],
        key_columns=["id_municipio"],
        measure_columns=[
            "taxa_alfabetizacao", "percentual_participacao",
            "meta_alfabetizacao_2024", "meta_alfabetizacao_2025",
            "meta_alfabetizacao_2026", "meta_alfabetizacao_2027",
            "meta_alfabetizacao_2028", "meta_alfabetizacao_2029",
            "meta_alfabetizacao_2030"
        ],
        dimension_columns=["ano", "id_municipio", "id_municipio_nome", "rede", "nivel_alfabetizacao"],
        uses_dictionary=False
    ),
}


solution_model = SolutionModel(
    cloud="Azure",
    runtime="Databricks Runtime 15.4",
    storage_format="Delta/Parquet",
    source_project="basedosdados.br_inep_avaliacao_alfabetizacao (BigQuery)",
    batch_entities=[
        "uf",
        "municipio",
        "alunos",
        "meta_alfabetizacao_brasil",
        "meta_alfabetizacao_uf",
        "meta_alfabetizacao_municipio",
    ],
    # Streaming simulará chegada incremental dos registros das metas e resultados
    streaming_simulated_entities=[
        "meta_alfabetizacao_brasil",   # atualizações de metas nacionais
        "meta_alfabetizacao_uf",       # atualizações de metas estaduais
        "meta_alfabetizacao_municipio",# atualizações de metas municipais
        "alunos",                      # novos resultados de avaliação
    ],
    layers={
        "bronze": LayerConfig(
            name="bronze",
            description="Dados brutos do BigQuery preservados com histórico e auditoria.",
            expected_entities=[
                "bronze_uf",
                "bronze_municipio",
                "bronze_alunos",
                "bronze_meta_brasil",
                "bronze_meta_uf",
                "bronze_meta_municipio",
            ]
        ),
        "silver": LayerConfig(
            name="silver",
            description="Padroniza, valida e integra entidades usando chaves territoriais.",
            expected_entities=[
                "silver_dim_uf",
                "silver_dim_municipio",
                "silver_indicadores_uf",
                "silver_indicadores_municipio",
                "silver_microdados_alunos",
                "silver_meta_brasil",
                "silver_meta_uf",
                "silver_meta_municipio",
            ]
        ),
        "gold": LayerConfig(
            name="gold",
            description="Produtos analíticos finais para BI, estatísticas e ML.",
            expected_entities=[
                "gold_indicador_municipio",
                "gold_gap_meta_resultado",
                "gold_evolucao_temporal",
                "gold_perfil_nivel_proficiencia",
                "gold_painel_executivo",
            ]
        )
    }
)

for name, schema in source_schemas.items():
    print(f"[{name}] grain={schema.grain} | keys={schema.key_columns} | dicionario={schema.uses_dictionary}")



# COMMAND ----------

# DBTITLE 1,Modelo de Pipeline
from typing import Any

# MODELO DE IMPLEMENTAÇÃO (blueprint)
# Baseado nos esquemas reais da Base dos Dados:
# basedosdados.br_inep_avaliacao_alfabetizacao
# Nenhum processamento real ocorre aqui.


# ---------------------------------------------------------------------------
# BRONZE: contratos de ingestão por tabela
# ---------------------------------------------------------------------------

BRONZE_CONTRACTS: Dict[str, Dict] = {
    "bronze_uf": {
        "source": "basedosdados.br_inep_avaliacao_alfabetizacao.uf",
        "mode": "batch",
        "grain": ["ano", "sigla_uf", "serie", "rede"],
        "key_join": "br_bd_diretorios_brasil.uf ON sigla_uf = sigla",
        "dictionary_columns": ["serie", "rede"],
        "measures": [
            "taxa_alfabetizacao", "media_portugues",
            "proporcao_aluno_nivel_0 .. proporcao_aluno_nivel_8"
        ],
    },
    "bronze_municipio": {
        "source": "basedosdados.br_inep_avaliacao_alfabetizacao.municipio",
        "mode": "batch",
        "grain": ["ano", "id_municipio", "serie", "rede"],
        "key_join": "br_bd_diretorios_brasil.municipio ON id_municipio",
        "dictionary_columns": ["serie", "rede"],
        "measures": [
            "taxa_alfabetizacao", "media_portugues",
            "proporcao_aluno_nivel_0 .. proporcao_aluno_nivel_8"
        ],
    },
    "bronze_alunos": {
        "source": "basedosdados.br_inep_avaliacao_alfabetizacao.alunos",
        "mode": "batch + streaming (incremental por ano/avaliação)",
        "grain": ["ano", "id_aluno"],
        "key_join": "br_bd_diretorios_brasil.municipio ON id_municipio",
        "dictionary_columns": ["serie", "rede", "presenca", "preenchimento_caderno", "alfabetizado"],
        "measures": ["proficiencia", "peso_aluno"],
        "extra_keys": ["id_escola", "id_municipio", "caderno"],
    },
    "bronze_meta_brasil": {
        "source": "basedosdados.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_brasil",
        "mode": "batch + streaming (atualização de metas)",
        "grain": ["ano", "rede"],
        "key_join": None,
        "dictionary_columns": [],
        "measures": [
            "taxa_alfabetizacao", "percentual_participacao",
            "meta_alfabetizacao_2024 .. meta_alfabetizacao_2030"
        ],
    },
    "bronze_meta_uf": {
        "source": "basedosdados.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_uf",
        "mode": "batch + streaming (atualização de metas)",
        "grain": ["ano", "sigla_uf", "rede"],
        "key_join": "br_bd_diretorios_brasil.uf ON sigla_uf = sigla",
        "dictionary_columns": [],
        "measures": [
            "taxa_alfabetizacao", "percentual_participacao",
            "meta_alfabetizacao_2024 .. meta_alfabetizacao_2030"
        ],
    },
    "bronze_meta_municipio": {
        "source": "basedosdados.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_municipio",
        "mode": "batch + streaming (atualização de metas)",
        "grain": ["ano", "id_municipio", "rede"],
        "key_join": "br_bd_diretorios_brasil.municipio ON id_municipio",
        "dictionary_columns": [],
        "measures": [
            "taxa_alfabetizacao", "percentual_participacao",
            "meta_alfabetizacao_2024 .. meta_alfabetizacao_2030",
            "nivel_alfabetizacao"
        ],
    },
}


# ---------------------------------------------------------------------------
# SILVER: transformações esperadas por entidade
# ---------------------------------------------------------------------------

SILVER_TRANSFORMATIONS: Dict[str, Dict] = {
    "silver_dim_uf": {
        "source": "bronze_uf + bronze_meta_uf",
        "operations": [
            "Normalizar sigla_uf e sigla_uf_nome",
            "Garantir unicidade por sigla_uf",
            "Remover linhas com sigla_uf nula",
        ],
    },
    "silver_dim_municipio": {
        "source": "bronze_municipio + bronze_meta_municipio + bronze_alunos",
        "operations": [
            "Normalizar id_municipio e id_municipio_nome",
            "Garantir unicidade por id_municipio",
            "Remover linhas com id_municipio nulo",
            "Validar relacionamento id_municipio <-> sigla_uf via dicionário territorial",
        ],
    },
    "silver_indicadores_uf": {
        "source": "bronze_uf",
        "operations": [
            "Decodificar dicionário de serie e rede",
            "Validar faixa válida de taxa_alfabetizacao e proporcoes (0-100)",
            "Garantir que a soma das proporcoes_nivel_0..8 ≈ 100 por linha",
            "Tratar valores nulos em media_portugues",
        ],
    },
    "silver_indicadores_municipio": {
        "source": "bronze_municipio",
        "operations": [
            "Decodificar dicionário de serie e rede",
            "Validar faixa válida de taxa_alfabetizacao e proporcoes (0-100)",
            "Garantir que a soma das proporcoes_nivel_0..8 ≈ 100 por linha",
            "Validar id_municipio com silver_dim_municipio",
        ],
    },
    "silver_microdados_alunos": {
        "source": "bronze_alunos",
        "operations": [
            "Decodificar dicionário de serie, rede, presenca, preenchimento_caderno, alfabetizado",
            "Remover registros com id_aluno nulo",
            "Validar id_municipio com silver_dim_municipio",
            "Remover duplicatas por (ano, id_aluno)",
            "Tratar nulos em proficiencia e peso_aluno",
        ],
    },
    "silver_meta_brasil": {
        "source": "bronze_meta_brasil",
        "operations": [
            "Validar unicidade por (ano, rede)",
            "Verificar presença de todas as colunas meta_2024..2030",
            "Garantir taxa_alfabetizacao e percentual_participacao não nulos",
        ],
    },
    "silver_meta_uf": {
        "source": "bronze_meta_uf",
        "operations": [
            "Validar unicidade por (ano, sigla_uf, rede)",
            "Validar sigla_uf com silver_dim_uf",
            "Verificar coerência com silver_meta_brasil por rede",
        ],
    },
    "silver_meta_municipio": {
        "source": "bronze_meta_municipio",
        "operations": [
            "Validar unicidade por (ano, id_municipio, rede)",
            "Validar id_municipio com silver_dim_municipio",
            "Verificar nivel_alfabetizacao não nulo",
        ],
    },
}


# ---------------------------------------------------------------------------
# GOLD: produtos analíticos finais
# ---------------------------------------------------------------------------

GOLD_PRODUCTS: Dict[str, Dict] = {
    "gold_indicador_municipio": {
        "source": "silver_indicadores_municipio + silver_dim_municipio",
        "grain": ["ano", "id_municipio", "rede", "serie"],
        "key_columns": ["ano", "id_municipio", "id_municipio_nome", "sigla_uf"],
        "measures": ["taxa_alfabetizacao", "media_portugues",
                     "proporcao_aluno_nivel_0 .. proporcao_aluno_nivel_8"],
        "use_case": "Dashboards regionais e rankings municipais de alfabetização",
    },
    "gold_gap_meta_resultado": {
        "source": "silver_meta_municipio + silver_indicadores_municipio",
        "grain": ["ano", "id_municipio", "rede"],
        "key_columns": ["ano", "id_municipio", "id_municipio_nome", "rede"],
        "measures": [
            "taxa_alfabetizacao (realizado)",
            "meta_alfabetizacao_2024 .. 2030 (planejado)",
            "gap = meta_ano_vigente - taxa_alfabetizacao",
            "percentual_participacao",
        ],
        "use_case": "Monitoramento de metas nacionais e comparativo de progresso",
    },
    "gold_evolucao_temporal": {
        "source": "silver_indicadores_uf + silver_indicadores_municipio",
        "grain": ["ano", "sigla_uf", "rede", "serie"],
        "key_columns": ["ano", "sigla_uf", "rede"],
        "measures": ["taxa_alfabetizacao", "media_portugues"],
        "use_case": "Série histórica para análise de tendência e modelos preditivos",
    },
    "gold_perfil_nivel_proficiencia": {
        "source": "silver_microdados_alunos + silver_dim_municipio",
        "grain": ["ano", "id_municipio", "rede", "serie"],
        "key_columns": ["ano", "id_municipio", "rede", "serie"],
        "measures": [
            "proporcao_por_nivel (0-8)",
            "proficiencia_media", "proficiencia_mediana",
            "percentual_alfabetizado"
        ],
        "use_case": "Análise de distribuição de proficiência e clusters de vulnerabilidade",
    },
    "gold_painel_executivo": {
        "source": "gold_indicador_municipio + gold_gap_meta_resultado + silver_meta_brasil",
        "grain": ["ano", "rede"],
        "key_columns": ["ano", "rede"],
        "measures": [
            "taxa_media_nacional", "meta_vigente", "gap_nacional",
            "percentual_municipios_acima_meta",
        ],
        "use_case": "Visão executiva para acompanhamento estratégico e polítíicas públicas",
    },
}


def end_to_end_logical_flow() -> Dict[str, Any]:
    """Encadeia o desenho lógico da solução sem executar processamento real."""
    return {
        "bronze": {k: v["source"] for k, v in BRONZE_CONTRACTS.items()},
        "silver": {k: v["source"] for k, v in SILVER_TRANSFORMATIONS.items()},
        "gold":   {k: v["use_case"] for k, v in GOLD_PRODUCTS.items()},
    }


logical_flow = end_to_end_logical_flow()
for layer, entities in logical_flow.items():
    print(f"\n=== {layer.upper()} ===")
    for entity, info in entities.items():
        print(f"  {entity}: {info}")



# COMMAND ----------

# DBTITLE 1,Modelo de Qualidade e Governanca
# REGRAS DE QUALIDADE E GOVERNANÇA
# Baseadas nos esquemas reais das tabelas da Base dos Dados

quality_rules = {
    "unicidade": [
        # uf
        "bronze_uf: (ano, sigla_uf, serie, rede) devem ser únicos.",
        # municipio
        "bronze_municipio: (ano, id_municipio, serie, rede) devem ser únicos.",
        # alunos
        "bronze_alunos: (ano, id_aluno) devem ser únicos.",
        # metas
        "bronze_meta_brasil: (ano, rede) devem ser únicos.",
        "bronze_meta_uf: (ano, sigla_uf, rede) devem ser únicos.",
        "bronze_meta_municipio: (ano, id_municipio, rede) devem ser únicos.",
    ],
    "completude": [
        # chaves obrigatórias
        "sigla_uf não pode ser nula em bronze_uf, bronze_meta_uf.",
        "id_municipio não pode ser nulo em bronze_municipio, bronze_meta_municipio, bronze_alunos.",
        "id_aluno não pode ser nulo em bronze_alunos.",
        "ano não pode ser nulo em nenhuma das tabelas.",
        "rede não pode ser nula nas tabelas de metas.",
        # medidas críticas
        "taxa_alfabetizacao não pode ser nula nas tabelas de indicadores e metas.",
        "proficiencia não pode ser nula em bronze_alunos (exceto ausentados).",
        "meta_alfabetizacao_2030 deve estar presente em todas as linhas de metas.",
    ],
    "integridade_referencial": [
        "sigla_uf em bronze_uf e bronze_meta_uf deve existir em br_bd_diretorios_brasil.uf.",
        "id_municipio em bronze_municipio, bronze_meta_municipio e bronze_alunos deve existir em br_bd_diretorios_brasil.municipio.",
        "id_escola em bronze_alunos deve ser verificado no contexto de id_municipio.",
        "Metas municipais devem ter id_municipio presente em silver_dim_municipio.",
    ],
    "consistencia": [
        # proporções de nível
        "proporcao_aluno_nivel_0..8: cada valor deve estar entre 0 e 100.",
        "Soma de proporcao_aluno_nivel_0..8 deve ser aproximadamente 100 (tolerancia de 1%).",
        # taxa e proficiência
        "taxa_alfabetizacao deve estar entre 0 e 100.",
        "proficiencia deve ser positiva em bronze_alunos.",
        "peso_aluno deve ser positivo e não nulo em bronze_alunos.",
        # metas
        "meta_alfabetizacao_2030 >= meta_alfabetizacao_2024 (trajetória crescente esperada).",
        "percentual_participacao deve estar entre 0 e 100.",
        # dicionário
        "Valores decodificados de serie e rede não podem ser nulos após join com dicionário.",
        "alfabetizado em bronze_alunos deve ter valor válido após decodificação.",
    ],
}


monitoring_kpis = {
    "ingestao": [
        "volume de linhas ingeridas por tabela e por execução",
        "taxa de falhas de extração do BigQuery",
        "latência entre extração e persistência na Bronze",
        "volume de eventos streaming processados (metas e alunos)",
    ],
    "qualidade": [
        "percentual de nulos por coluna crítica (taxa_alfabetizacao, id_municipio, sigla_uf)",
        "número de duplicatas por tabela e por grain",
        "quebras de integridade referencial (id_municipio/sigla_uf não encontrados)",
        "proporções fora da faixa válida",
        "linhas com soma de proporcao_nivel fora da tolerância",
    ],
    "operacao": [
        "tempo de processamento por camada (Bronze, Silver, Gold)",
        "número de reprocessamentos por etapa",
        "freshness: data da última carga por tabela",
    ],
    "custos": [
        "volume de dados armazenados por camada (Bronze, Silver, Gold)",
        "custo estimado por execução batch",
        "custo estimado por evento streaming",
    ],
}


finops_principles = [
    "Persistência em Delta/Parquet com compressão colunar.",
    "Particionar bronze_alunos por ano (tabela de maior volume).",
    "Evitar particionar tabelas pequenas como bronze_meta_brasil.",
    "Isolar cargas batch de alunos (maior volume) das cargas de metas.",
    "Materializar Gold apenas para os produtos analíticos efetivamente consumidos.",
    "Reutilizar silver_dim_municipio como join broadcast para evitar shuffles.",
    "Limitar a janela de streaming incremental a (ano, rede) para reduzir reprocessamento.",
]


print("=== QUALITY RULES ===")
for category, rules in quality_rules.items():
    print(f"\n[{category}]")
    for r in rules:
        print(f"  - {r}")

print("\n=== MONITORING KPIs ===")
for category, kpis in monitoring_kpis.items():
    print(f"\n[{category}]")
    for k in kpis:
        print(f"  - {k}")

print("\n=== FINOPS ===")
for p in finops_principles:
    print(f"  - {p}")



# COMMAND ----------

# DBTITLE 1,Estrutura de Entrega
# MAGIC %md
# MAGIC # Estrutura mínima recomendada
# MAGIC
# MAGIC ## Repositório
# MAGIC * `README.md`: visão executiva e técnica da solução
# MAGIC * `modelo_solucao.md`: detalhamento do desenho lógico, camadas e contratos
# MAGIC * `notebook`: blueprint funcional da solução
# MAGIC * `pipelines/bronze`, `pipelines/silver`, `pipelines/gold`: implementação futura
# MAGIC * `quality/`: validações e testes de qualidade
# MAGIC * `docs/`: diagramas e documentação complementar
# MAGIC
# MAGIC ## Decisões arquiteturais registradas
# MAGIC * **Batch + streaming** para equilibrar histórico e atualização frequente
# MAGIC * **Arquitetura medalhão** para separar confiabilidade operacional e consumo analítico
# MAGIC * **Data lakehouse** para reduzir custo de integração entre engenharia, analytics e IA
# MAGIC * **Camada Gold** desenhada para dashboards, análises estatísticas e modelos preditivos
# MAGIC
# MAGIC ## Como evoluir este modelo
# MAGIC 1. Mapear as tabelas reais na Base dos Dados.
# MAGIC 2. Implementar ingestão batch e simulação streaming.
# MAGIC 3. Codificar regras de qualidade com testes automatizados.
# MAGIC 4. Materializar produtos Gold aderentes aos dashboards e análises.
# MAGIC 5. Publicar documentação final, histórico Git e evidências de monitoramento.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Implementacao em Memoria
# MAGIC %md
# MAGIC # Implementação em Memória (Teste)
# MAGIC
# MAGIC As células a seguir implementam o pipeline completo **em memória**, sem criar tabelas Delta, jobs ou outros recursos.
# MAGIC
# MAGIC ## Escopo da implementação
# MAGIC * **Fixtures Bronze**: DataFrames simulando os dados extraídos do BigQuery
# MAGIC * **Transformações Silver**: limpeza, decodificação de dicionários, validação de integridade
# MAGIC * **Produtos Gold**: agregações e métricas finais
# MAGIC * **Validação de Qualidade**: verificação de regras de unicidade, completude, integridade e consistência
# MAGIC
# MAGIC ## Objetivo
# MAGIC Validar a lógica do pipeline antes de provisionar recursos em produção.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Setup e Fixtures Bronze
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)
from typing import Dict, List, Tuple
import datetime

# =============================================================================
# DICIONÁRIOS DE DECODIFICAÇÃO (simulando br_inep_avaliacao_alfabetizacao.dicionario)
# =============================================================================

DICT_SERIE = {"2": "2º Ano EF"}
DICT_REDE = {"1": "Municipal", "2": "Estadual", "4": "Privada"}
DICT_PRESENCA = {"1": "Presente", "0": "Ausente"}
DICT_PREENCHIMENTO = {"1": "Preenchido", "0": "Não preenchido"}
DICT_ALFABETIZADO = {"1": "Alfabetizado", "0": "Não alfabetizado"}

# =============================================================================
# FIXTURES: DADOS MOCK BRONZE (simulando extração do BigQuery)
# =============================================================================

# --- bronze_uf ---
bronze_uf_schema = StructType([
    StructField("ano", IntegerType()),
    StructField("sigla_uf", StringType()),
    StructField("sigla_uf_nome", StringType()),
    StructField("serie", StringType()),
    StructField("rede", StringType()),
    StructField("taxa_alfabetizacao", DoubleType()),
    StructField("media_portugues", DoubleType()),
    StructField("proporcao_aluno_nivel_0", DoubleType()),
    StructField("proporcao_aluno_nivel_1", DoubleType()),
    StructField("proporcao_aluno_nivel_2", DoubleType()),
    StructField("proporcao_aluno_nivel_3", DoubleType()),
    StructField("proporcao_aluno_nivel_4", DoubleType()),
    StructField("proporcao_aluno_nivel_5", DoubleType()),
    StructField("proporcao_aluno_nivel_6", DoubleType()),
    StructField("proporcao_aluno_nivel_7", DoubleType()),
    StructField("proporcao_aluno_nivel_8", DoubleType()),
])

bronze_uf_data = [
    (2023, "SP", "São Paulo", "2", "1", 78.5, 720.3, 2.1, 5.3, 8.2, 12.4, 15.6, 18.9, 22.1, 10.5, 4.9),
    (2023, "RJ", "Rio de Janeiro", "2", "1", 72.1, 698.4, 3.2, 6.1, 9.5, 13.8, 16.2, 17.5, 20.3, 9.8, 3.6),
    (2023, "MG", "Minas Gerais", "2", "1", 75.3, 710.2, 2.8, 5.8, 8.9, 13.1, 15.9, 18.2, 21.5, 10.1, 3.7),
    (2023, "SP", "São Paulo", "2", "4", 89.2, 780.5, 0.8, 2.1, 4.3, 8.5, 12.7, 18.9, 25.3, 18.6, 8.8),
    # Dado com problema de qualidade: sigla_uf nula
    (2023, None, None, "2", "1", 70.0, 690.0, 3.0, 6.0, 9.0, 13.0, 16.0, 18.0, 21.0, 10.0, 4.0),
]

bronze_uf_df = spark.createDataFrame(bronze_uf_data, bronze_uf_schema)

# --- bronze_municipio ---
bronze_municipio_schema = StructType([
    StructField("ano", IntegerType()),
    StructField("id_municipio", StringType()),
    StructField("id_municipio_nome", StringType()),
    StructField("serie", StringType()),
    StructField("rede", StringType()),
    StructField("taxa_alfabetizacao", DoubleType()),
    StructField("media_portugues", DoubleType()),
    StructField("proporcao_aluno_nivel_0", DoubleType()),
    StructField("proporcao_aluno_nivel_1", DoubleType()),
    StructField("proporcao_aluno_nivel_2", DoubleType()),
    StructField("proporcao_aluno_nivel_3", DoubleType()),
    StructField("proporcao_aluno_nivel_4", DoubleType()),
    StructField("proporcao_aluno_nivel_5", DoubleType()),
    StructField("proporcao_aluno_nivel_6", DoubleType()),
    StructField("proporcao_aluno_nivel_7", DoubleType()),
    StructField("proporcao_aluno_nivel_8", DoubleType()),
])

bronze_municipio_data = [
    (2023, "3550308", "São Paulo", "2", "1", 79.2, 725.1, 2.0, 5.1, 8.0, 12.2, 15.4, 19.1, 22.5, 10.8, 4.9),
    (2023, "3304557", "Rio de Janeiro", "2", "1", 71.5, 695.8, 3.4, 6.3, 9.7, 14.0, 16.4, 17.3, 20.1, 9.5, 3.3),
    (2023, "3106200", "Belo Horizonte", "2", "1", 76.1, 715.3, 2.6, 5.6, 8.7, 12.9, 15.7, 18.4, 21.8, 10.3, 4.0),
    (2023, "3550308", "São Paulo", "2", "4", 90.1, 785.2, 0.7, 2.0, 4.1, 8.3, 12.5, 19.1, 25.6, 18.8, 8.9),
    # Duplicata intencional para teste de dedup
    (2023, "3550308", "São Paulo", "2", "1", 79.2, 725.1, 2.0, 5.1, 8.0, 12.2, 15.4, 19.1, 22.5, 10.8, 4.9),
    # Dado com id_municipio nulo
    (2023, None, None, "2", "1", 65.0, 680.0, 4.0, 7.0, 10.0, 14.0, 16.0, 17.0, 19.0, 9.0, 4.0),
]

bronze_municipio_df = spark.createDataFrame(bronze_municipio_data, bronze_municipio_schema)

# --- bronze_alunos ---
bronze_alunos_schema = StructType([
    StructField("ano", IntegerType()),
    StructField("id_municipio", StringType()),
    StructField("id_municipio_nome", StringType()),
    StructField("id_escola", StringType()),
    StructField("id_aluno", StringType()),
    StructField("caderno", StringType()),
    StructField("serie", StringType()),
    StructField("rede", StringType()),
    StructField("presenca", StringType()),
    StructField("preenchimento_caderno", StringType()),
    StructField("alfabetizado", StringType()),
    StructField("proficiencia", DoubleType()),
    StructField("peso_aluno", DoubleType()),
])

bronze_alunos_data = [
    (2023, "3550308", "São Paulo", "35001234", "A001", "C1", "2", "1", "1", "1", "1", 780.5, 1.2),
    (2023, "3550308", "São Paulo", "35001234", "A002", "C1", "2", "1", "1", "1", "1", 765.2, 1.1),
    (2023, "3550308", "São Paulo", "35001234", "A003", "C2", "2", "1", "1", "1", "0", 710.3, 1.3),
    (2023, "3304557", "Rio de Janeiro", "33005678", "A004", "C1", "2", "1", "1", "1", "1", 755.8, 1.0),
    (2023, "3304557", "Rio de Janeiro", "33005678", "A005", "C1", "2", "1", "0", "0", None, None, 1.0),  # ausente
    (2023, "3106200", "Belo Horizonte", "31009012", "A006", "C2", "2", "1", "1", "1", "1", 790.1, 1.15),
    # Duplicata intencional
    (2023, "3550308", "São Paulo", "35001234", "A001", "C1", "2", "1", "1", "1", "1", 780.5, 1.2),
]

bronze_alunos_df = spark.createDataFrame(bronze_alunos_data, bronze_alunos_schema)

# --- bronze_meta_brasil ---
bronze_meta_brasil_schema = StructType([
    StructField("ano", IntegerType()),
    StructField("rede", StringType()),
    StructField("taxa_alfabetizacao", DoubleType()),
    StructField("meta_alfabetizacao_2024", DoubleType()),
    StructField("meta_alfabetizacao_2025", DoubleType()),
    StructField("meta_alfabetizacao_2026", DoubleType()),
    StructField("meta_alfabetizacao_2027", DoubleType()),
    StructField("meta_alfabetizacao_2028", DoubleType()),
    StructField("meta_alfabetizacao_2029", DoubleType()),
    StructField("meta_alfabetizacao_2030", DoubleType()),
    StructField("percentual_participacao", DoubleType()),
])

bronze_meta_brasil_data = [
    (2023, "1", 56.0, 60.0, 65.0, 70.0, 75.0, 82.0, 90.0, 100.0, 85.2),
    (2023, "2", 62.0, 66.0, 71.0, 76.0, 81.0, 87.0, 93.0, 100.0, 88.5),
    (2023, "4", 78.0, 80.0, 83.0, 86.0, 90.0, 94.0, 97.0, 100.0, 92.1),
]

bronze_meta_brasil_df = spark.createDataFrame(bronze_meta_brasil_data, bronze_meta_brasil_schema)

# --- bronze_meta_uf ---
bronze_meta_uf_schema = StructType([
    StructField("ano", IntegerType()),
    StructField("sigla_uf", StringType()),
    StructField("sigla_uf_nome", StringType()),
    StructField("rede", StringType()),
    StructField("taxa_alfabetizacao", DoubleType()),
    StructField("meta_alfabetizacao_2024", DoubleType()),
    StructField("meta_alfabetizacao_2025", DoubleType()),
    StructField("meta_alfabetizacao_2026", DoubleType()),
    StructField("meta_alfabetizacao_2027", DoubleType()),
    StructField("meta_alfabetizacao_2028", DoubleType()),
    StructField("meta_alfabetizacao_2029", DoubleType()),
    StructField("meta_alfabetizacao_2030", DoubleType()),
    StructField("percentual_participacao", DoubleType()),
])

bronze_meta_uf_data = [
    (2023, "SP", "São Paulo", "1", 78.5, 80.0, 83.0, 86.0, 89.0, 93.0, 97.0, 100.0, 89.5),
    (2023, "RJ", "Rio de Janeiro", "1", 72.1, 75.0, 79.0, 83.0, 87.0, 91.0, 96.0, 100.0, 86.2),
    (2023, "MG", "Minas Gerais", "1", 75.3, 78.0, 81.0, 85.0, 88.0, 92.0, 96.0, 100.0, 87.8),
]

bronze_meta_uf_df = spark.createDataFrame(bronze_meta_uf_data, bronze_meta_uf_schema)

# --- bronze_meta_municipio ---
bronze_meta_municipio_schema = StructType([
    StructField("ano", IntegerType()),
    StructField("id_municipio", StringType()),
    StructField("id_municipio_nome", StringType()),
    StructField("rede", StringType()),
    StructField("taxa_alfabetizacao", DoubleType()),
    StructField("meta_alfabetizacao_2024", DoubleType()),
    StructField("meta_alfabetizacao_2025", DoubleType()),
    StructField("meta_alfabetizacao_2026", DoubleType()),
    StructField("meta_alfabetizacao_2027", DoubleType()),
    StructField("meta_alfabetizacao_2028", DoubleType()),
    StructField("meta_alfabetizacao_2029", DoubleType()),
    StructField("meta_alfabetizacao_2030", DoubleType()),
    StructField("nivel_alfabetizacao", StringType()),
    StructField("percentual_participacao", DoubleType()),
])

bronze_meta_municipio_data = [
    (2023, "3550308", "São Paulo", "1", 79.2, 81.0, 84.0, 87.0, 90.0, 94.0, 97.0, 100.0, "Alto", 90.1),
    (2023, "3304557", "Rio de Janeiro", "1", 71.5, 74.0, 78.0, 82.0, 86.0, 91.0, 96.0, 100.0, "Médio", 85.3),
    (2023, "3106200", "Belo Horizonte", "1", 76.1, 79.0, 82.0, 85.0, 89.0, 93.0, 97.0, 100.0, "Alto", 88.7),
]

bronze_meta_municipio_df = spark.createDataFrame(bronze_meta_municipio_data, bronze_meta_municipio_schema)

# Consolidar todas as tabelas Bronze em um dicionário
BRONZE_TABLES: Dict[str, DataFrame] = {
    "bronze_uf": bronze_uf_df,
    "bronze_municipio": bronze_municipio_df,
    "bronze_alunos": bronze_alunos_df,
    "bronze_meta_brasil": bronze_meta_brasil_df,
    "bronze_meta_uf": bronze_meta_uf_df,
    "bronze_meta_municipio": bronze_meta_municipio_df,
}

print("=== BRONZE TABLES LOADED ===")
for name, df in BRONZE_TABLES.items():
    print(f"  {name}: {df.count()} rows")


# COMMAND ----------

# DBTITLE 1,Transformacoes Silver
# =============================================================================
# TRANSFORMAÇÕES SILVER
# =============================================================================

def decode_dictionary(df: DataFrame, col_name: str, mapping: Dict[str, str]) -> DataFrame:
    """Decodifica coluna usando dicionário de mapeamento."""
    mapping_expr = F.create_map([F.lit(x) for kv in mapping.items() for x in kv])
    return df.withColumn(
        f"{col_name}_decoded",
        F.coalesce(mapping_expr[F.col(col_name)], F.lit("Desconhecido"))
    )


def transform_silver_dim_uf(bronze_uf: DataFrame, bronze_meta_uf: DataFrame) -> DataFrame:
    """Cria dimensão de UF normalizada."""
    uf_from_indicadores = bronze_uf.select("sigla_uf", "sigla_uf_nome").distinct()
    uf_from_metas = bronze_meta_uf.select("sigla_uf", "sigla_uf_nome").distinct()
    
    return (
        uf_from_indicadores.union(uf_from_metas)
        .filter(F.col("sigla_uf").isNotNull())
        .dropDuplicates(["sigla_uf"])
        .orderBy("sigla_uf")
    )


def transform_silver_dim_municipio(bronze_mun: DataFrame, bronze_meta_mun: DataFrame) -> DataFrame:
    """Cria dimensão de município normalizada."""
    mun_from_indicadores = bronze_mun.select("id_municipio", "id_municipio_nome").distinct()
    mun_from_metas = bronze_meta_mun.select("id_municipio", "id_municipio_nome").distinct()
    
    return (
        mun_from_indicadores.union(mun_from_metas)
        .filter(F.col("id_municipio").isNotNull())
        .dropDuplicates(["id_municipio"])
        .orderBy("id_municipio")
    )


def transform_silver_indicadores_uf(bronze_uf: DataFrame) -> DataFrame:
    """Transforma indicadores de UF: decodifica dicionário, valida, deduplica."""
    df = bronze_uf.filter(F.col("sigla_uf").isNotNull())
    df = decode_dictionary(df, "serie", DICT_SERIE)
    df = decode_dictionary(df, "rede", DICT_REDE)
    
    # Calcula soma das proporções para validação
    proporcao_cols = [f"proporcao_aluno_nivel_{i}" for i in range(9)]
    df = df.withColumn(
        "soma_proporcoes",
        sum(F.coalesce(F.col(c), F.lit(0.0)) for c in proporcao_cols)
    )
    
    # Valida faixa (0-100) para taxa_alfabetizacao
    df = df.withColumn(
        "taxa_alfabetizacao_valida",
        F.when((F.col("taxa_alfabetizacao") >= 0) & (F.col("taxa_alfabetizacao") <= 100), True).otherwise(False)
    )
    
    return df.dropDuplicates(["ano", "sigla_uf", "serie", "rede"])


def transform_silver_indicadores_municipio(bronze_mun: DataFrame, dim_municipio: DataFrame) -> DataFrame:
    """Transforma indicadores de município."""
    df = bronze_mun.filter(F.col("id_municipio").isNotNull())
    df = decode_dictionary(df, "serie", DICT_SERIE)
    df = decode_dictionary(df, "rede", DICT_REDE)
    
    # Calcula soma das proporções
    proporcao_cols = [f"proporcao_aluno_nivel_{i}" for i in range(9)]
    df = df.withColumn(
        "soma_proporcoes",
        sum(F.coalesce(F.col(c), F.lit(0.0)) for c in proporcao_cols)
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
    """Transforma microdados de alunos: decodifica 5 colunas, deduplica, valida."""
    df = bronze_alunos.filter(F.col("id_aluno").isNotNull())
    
    # Decodifica todas as colunas de dicionário
    df = decode_dictionary(df, "serie", DICT_SERIE)
    df = decode_dictionary(df, "rede", DICT_REDE)
    df = decode_dictionary(df, "presenca", DICT_PRESENCA)
    df = decode_dictionary(df, "preenchimento_caderno", DICT_PREENCHIMENTO)
    df = decode_dictionary(df, "alfabetizado", DICT_ALFABETIZADO)
    
    # Valida integridade referencial
    df = df.join(
        dim_municipio.select(F.col("id_municipio").alias("dim_id_municipio")),
        df.id_municipio == F.col("dim_id_municipio"),
        "left"
    ).withColumn(
        "municipio_valido",
        F.col("dim_id_municipio").isNotNull()
    ).drop("dim_id_municipio")
    
    # Remove duplicatas por grain
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
    
    # Valida integridade referencial
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
silver_dim_municipio = transform_silver_dim_municipio(bronze_municipio_df, bronze_meta_municipio_df)
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
            "ano", "id_municipio", "id_municipio_nome",
            "serie_decoded", "rede_decoded",
            "taxa_alfabetizacao", "media_portugues",
            "proporcao_aluno_nivel_0", "proporcao_aluno_nivel_1",
            "proporcao_aluno_nivel_2", "proporcao_aluno_nivel_3",
            "proporcao_aluno_nivel_4", "proporcao_aluno_nivel_5",
            "proporcao_aluno_nivel_6", "proporcao_aluno_nivel_7",
            "proporcao_aluno_nivel_8",
            "soma_proporcoes", "municipio_valido"
        )
        .withColumnRenamed("serie_decoded", "serie")
        .withColumnRenamed("rede_decoded", "rede")
    )


def build_gold_gap_meta_resultado(
    silver_indicadores_mun: DataFrame,
    silver_meta_mun: DataFrame
) -> DataFrame:
    """Gold: Gap entre meta planejada e resultado observado por município."""
    indicadores = silver_indicadores_mun.select(
        "ano", "id_municipio", "id_municipio_nome", "rede",
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
        "inner"
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
            "ano", "sigla_uf", "sigla_uf_nome",
            "serie_decoded", "rede_decoded",
            "taxa_alfabetizacao", "media_portugues"
        )
        .withColumnRenamed("serie_decoded", "serie")
        .withColumnRenamed("rede_decoded", "rede")
        .orderBy("ano", "sigla_uf", "rede")
    )


def build_gold_perfil_proficiencia(
    silver_alunos: DataFrame,
    silver_dim_mun: DataFrame
) -> DataFrame:
    """Gold: Perfil de proficiência agregado por município."""
    # Filtrar apenas alunos presentes com proficiência válida
    alunos_presentes = silver_alunos.filter(
        (F.col("presenca") == "1") & F.col("proficiencia").isNotNull()
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
            F.sum(F.when(F.col("alfabetizado") == "1", 1).otherwise(0)).alias("qtd_alfabetizados"),
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
    """Verifica se soma das proporções está próxima de 100."""
    total = df.filter(F.col("soma_proporcoes").isNotNull()).count()
    out_of_tolerance = df.filter(
        (F.col("soma_proporcoes") < 100 - tolerance) | (F.col("soma_proporcoes") > 100 + tolerance)
    ).count()
    passed = out_of_tolerance == 0
    return QualityCheckResult(
        check_name="Consistência (soma proporções)",
        table_name=table_name,
        passed=passed,
        total_rows=total,
        failed_rows=out_of_tolerance,
        message=f"Fora de tolerância ({tolerance}%): {out_of_tolerance}"
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


# --- Executar todas as validações ---
quality_results: List[QualityCheckResult] = []

# Bronze - Unicidade (nos dados brutos, espera-se duplicatas como teste)
quality_results.append(check_uniqueness(bronze_uf_df, "bronze_uf", ["ano", "sigla_uf", "serie", "rede"]))
quality_results.append(check_uniqueness(bronze_municipio_df, "bronze_municipio", ["ano", "id_municipio", "serie", "rede"]))
quality_results.append(check_uniqueness(bronze_alunos_df, "bronze_alunos", ["ano", "id_aluno"]))

# Bronze - Completude
quality_results.append(check_not_null(bronze_uf_df, "bronze_uf", "sigla_uf"))
quality_results.append(check_not_null(bronze_municipio_df, "bronze_municipio", "id_municipio"))
quality_results.append(check_not_null(bronze_alunos_df, "bronze_alunos", "id_aluno"))

# Silver - Unicidade (após dedup)
quality_results.append(check_uniqueness(silver_indicadores_uf, "silver_indicadores_uf", ["ano", "sigla_uf", "serie", "rede"]))
quality_results.append(check_uniqueness(silver_indicadores_municipio, "silver_indicadores_municipio", ["ano", "id_municipio", "serie", "rede"]))
quality_results.append(check_uniqueness(silver_microdados_alunos, "silver_microdados_alunos", ["ano", "id_aluno"]))

# Silver - Consistência
quality_results.append(check_range(silver_indicadores_uf, "silver_indicadores_uf", "taxa_alfabetizacao", 0, 100))
quality_results.append(check_range(silver_indicadores_municipio, "silver_indicadores_municipio", "taxa_alfabetizacao", 0, 100))
quality_results.append(check_proportion_sum(silver_indicadores_uf, "silver_indicadores_uf", tolerance=5.0))
quality_results.append(check_proportion_sum(silver_indicadores_municipio, "silver_indicadores_municipio", tolerance=5.0))

# Silver - Integridade referencial
quality_results.append(check_referential_integrity(silver_indicadores_municipio, "silver_indicadores_municipio", "municipio_valido"))
quality_results.append(check_referential_integrity(silver_microdados_alunos, "silver_microdados_alunos", "municipio_valido"))
quality_results.append(check_referential_integrity(silver_meta_uf, "silver_meta_uf", "uf_valida"))
quality_results.append(check_referential_integrity(silver_meta_municipio, "silver_meta_municipio", "municipio_valido"))

# Silver - Metas monotônicas
quality_results.append(check_monotonic_metas(silver_meta_brasil, "silver_meta_brasil"))
quality_results.append(check_monotonic_metas(silver_meta_uf, "silver_meta_uf"))
quality_results.append(check_monotonic_metas(silver_meta_municipio, "silver_meta_municipio"))

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
