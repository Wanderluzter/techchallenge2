# Tech Challenge - Fase 2

## Pipeline Híbrida para Análise da Alfabetização no Brasil

Este repositório apresenta o **modelo da solução** para uma pipeline híbrida de dados voltada à análise do indicador de alfabetização no Brasil, conforme as diretrizes do Tech Challenge. A proposta foi desenhada **sem provisionamento de recursos**, servindo como base arquitetural, documental e de notebook para futura implementação em Databricks no Azure.

---

## 1. Contexto do problema

A alfabetização na infância é um dos pilares fundamentais para o desenvolvimento educacional, social e econômico do país. O **Compromisso Nacional Criança Alfabetizada** é uma política pública que mobiliza União, estados, Distrito Federal e municípios com o objetivo de garantir que todas as crianças brasileiras estejam alfabetizadas até o final do 2º ano do ensino fundamental.

Em 2023, o **Instituto Nacional de Estudos e Pesquisas Educacionais Anísio Teixeira (INEP)** realizou a **Pesquisa Alfabetiza Brasil**, que definiu o ponto de corte de **743 pontos na escala de proficiência do Saeb** como referência para considerar uma criança alfabetizada. A partir desse parâmetro, foi criado o **Indicador Criança Alfabetizada**, que expressa o percentual de estudantes que atingem esse patamar.

A **meta nacional** é que, até **2030**, todas as crianças brasileiras estejam alfabetizadas ao final do 2º ano do ensino fundamental.

Para compreender os fatores que influenciam esse processo, é necessário integrar múltiplas fontes de dados educacionais, territoriais e socioeconômicos. Este projeto utiliza os dados disponíveis na plataforma [Base dos Dados](https://basedosdados.org/dataset/073a39d4-89cf-4068-b1e8-34ed0d9c0b72?table=e1de7a6a-5038-4e81-89f0-a15f2cc12c9b), especificamente o conjunto `br_inep_avaliacao_alfabetizacao`.

---

## 2. Objetivo da solução

Construir o desenho de uma pipeline escalável em nuvem que realize:

* ingestão de diferentes fontes de dados educacionais;
* tratamento e padronização das informações;
* integração entre bases heterogêneas;
* disponibilização de uma camada analítica confiável;
* monitoramento operacional do pipeline;
* controle de custos da infraestrutura.

---

## 3. Fontes de dados

### Fontes principais (Base dos Dados — `basedosdados.br_inep_avaliacao_alfabetizacao`)

| Tabela BigQuery | Grain | Modo de ingestão |
| --- | --- | --- |
| `uf` | ano, sigla_uf, serie, rede | Batch |
| `municipio` | ano, id_municipio, serie, rede | Batch |
| `alunos` | ano, id_aluno | Batch + Streaming |
| `meta_alfabetizacao_brasil` | ano, rede | Batch + Streaming |
| `meta_alfabetizacao_uf` | ano, sigla_uf, rede | Batch + Streaming |
| `meta_alfabetizacao_municipio` | ano, id_municipio, rede | Batch + Streaming |

Todas as tabelas com código categórico (`serie`, `rede`, `presenca`, `preenchimento_caderno`, `alfabetizado`) utilizam join com `br_inep_avaliacao_alfabetizacao.dicionario` para decodificação dos valores.

### Fontes externas opcionais (enriquecimento futuro)

| Fonte | Conteúdo | Uso previsto |
| --- | --- | --- |
| Censo Escolar (INEP) | Estrutura escolar | Infraestrutura e capacidade de atendimento |
| IBGE — Censo / PNAD | Dados socioeconômicos | Contexto de renda e acesso |
| Atlas do Desenvolvimento Humano | IDH municipal | Correlação com proficiência |
| Cadastro Único / Bolsa Família | Vulnerabilidade social | Clusters de risco educacional |
| IBGE — Malha territorial | Território e limites | Enriquecimento geoespacial |
| FUNDEB | Financiamento educacional | Relação custo-resultado por rede |

---

## 4. Arquitetura proposta

A solução adota um padrão híbrido de processamento com arquitetura medalhão:

### Ingestão batch
Cargas periódicas para dados históricos, cadastrais e agregados de baixa frequência de atualização: metas nacionais/estaduais/municipais, cadastros de UF e municípios, microdados históricos de alunos.

### Ingestão streaming
Simulação de ingestão quase em tempo real para: novas medições de desempenho (`alunos`), atualização de metas (`meta_alfabetizacao_brasil`, `meta_alfabetizacao_uf`, `meta_alfabetizacao_municipio`) e revisão de resultados.

### Arquitetura medalhão

* **Bronze**: armazenamento bruto, histórico completo e auditável, sem transformações significativas.
* **Silver**: limpeza, padronização, deduplicação, validação de integridade e integração das entidades.
* **Gold**: datasets analíticos prontos para dashboards, análises estatísticas e machine learning.

---

## 5. Diagrama da pipeline

```
┌──────────────────────────────────────────────────────────┐
│              FONTES (Base dos Dados / BigQuery)          │
│  uf │ municipio │ alunos │ meta_brasil │ meta_uf │ meta_mun │
└───────────────────────┬──────────────────────────────────┘
                        │
          ┌─────────────┴─────────────┐
          │  INGESTÃO HÍBRIDA         │
          │  Batch: uf, municipio,    │
          │         alunos (hist.),   │
          │         metas (hist.)     │
          │  Streaming: alunos,       │
          │         metas (eventos)   │
          └─────────────┬─────────────┘
                        │
          ┌─────────────▼─────────────┐
          │   BRONZE (dados brutos)   │
          │  bronze_uf                │
          │  bronze_municipio         │
          │  bronze_alunos            │
          │  bronze_meta_brasil       │
          │  bronze_meta_uf           │
          │  bronze_meta_municipio    │
          └─────────────┬─────────────┘
                        │
          ┌─────────────▼─────────────┐
          │   SILVER (tratado)        │
          │  silver_dim_uf            │
          │  silver_dim_municipio     │
          │  silver_indicadores_uf    │
          │  silver_indicadores_mun   │
          │  silver_microdados_alunos │
          │  silver_meta_brasil/uf/mun│
          └─────────────┬─────────────┘
                        │
          ┌─────────────▼─────────────┐
          │   GOLD (analítico)        │
          │  gold_indicador_municipio │
          │  gold_gap_meta_resultado  │
          │  gold_evolucao_temporal   │
          │  gold_perfil_proficiencia │
          │  gold_painel_executivo    │
          └─────────────┬─────────────┘
                        │
          ┌─────────────▼─────────────┐
          │   CONSUMO                 │
          │  Dashboards │ Analytics   │
          │  Modelos ML │ Políticas   │
          └───────────────────────────┘
```

---

## 6. Modelo das camadas

### Bronze
Persistência dos dados brutos exatamente como extraídos do BigQuery, com auditoria de origem e timestamp de carga.

Entidades:

* `bronze_uf` — grain: (ano, sigla_uf, serie, rede)
* `bronze_municipio` — grain: (ano, id_municipio, serie, rede)
* `bronze_alunos` — grain: (ano, id_aluno)
* `bronze_meta_brasil` — grain: (ano, rede)
* `bronze_meta_uf` — grain: (ano, sigla_uf, rede)
* `bronze_meta_municipio` — grain: (ano, id_municipio, rede)

### Silver
Limpeza, decodificação de dicionários, padronização e integração entre entidades.

Transformações esperadas:

* decodificação de `serie`, `rede`, `presenca`, `preenchimento_caderno`, `alfabetizado` via tabela dicionário;
* normalização de `sigla_uf` e `id_municipio` com diretório territorial;
* remoção de duplicatas por grain definido;
* tratamento de nulos em `proficiencia`, `taxa_alfabetizacao` e `media_portugues`;
* validação de `proporcao_aluno_nivel_0..8` (soma ≈ 100 por linha);
* integridade referencial município → UF.

Entidades:

* `silver_dim_uf`
* `silver_dim_municipio`
* `silver_indicadores_uf`
* `silver_indicadores_municipio`
* `silver_microdados_alunos`
* `silver_meta_brasil`
* `silver_meta_uf`
* `silver_meta_municipio`

### Gold
Produtos analíticos finais para consumo por BI, análise estatística e modelos preditivos.

| Produto | Grain | Finalidade |
| --- | --- | --- |
| `gold_indicador_municipio` | ano, id_municipio, rede, serie | Dashboards regionais e rankings |
| `gold_gap_meta_resultado` | ano, id_municipio, rede | Monitoramento de metas e progresso |
| `gold_evolucao_temporal` | ano, sigla_uf, rede, serie | Série histórica e modelos preditivos |
| `gold_perfil_nivel_proficiencia` | ano, id_municipio, rede, serie | Distribuição de proficiência e clusters |
| `gold_painel_executivo` | ano, rede | Visão executiva e políticas públicas |

---

## 7. Regras de qualidade de dados

### Unicidade

| Tabela | Grain único |
| --- | --- |
| bronze_uf | (ano, sigla_uf, serie, rede) |
| bronze_municipio | (ano, id_municipio, serie, rede) |
| bronze_alunos | (ano, id_aluno) |
| bronze_meta_brasil | (ano, rede) |
| bronze_meta_uf | (ano, sigla_uf, rede) |
| bronze_meta_municipio | (ano, id_municipio, rede) |

### Completude

* `sigla_uf` obrigatória em `bronze_uf` e `bronze_meta_uf`.
* `id_municipio` obrigatório em `bronze_municipio`, `bronze_meta_municipio` e `bronze_alunos`.
* `id_aluno` obrigatório em `bronze_alunos`.
* `ano` obrigatório em todas as tabelas.
* `taxa_alfabetizacao` obrigatória nas tabelas de indicadores e metas.
* `meta_alfabetizacao_2024..2030` obrigatórias nas tabelas de meta.

### Integridade referencial

* `sigla_uf` deve existir em `br_bd_diretorios_brasil.uf`.
* `id_municipio` deve existir em `br_bd_diretorios_brasil.municipio`.
* Metas municipais devem ter `id_municipio` presente em `silver_dim_municipio`.

### Consistência

* `proporcao_aluno_nivel_0..8`: valores entre 0 e 100; soma ≈ 100 por linha (tolerância 1%).
* `taxa_alfabetizacao` e `percentual_participacao` entre 0 e 100.
* `proficiencia` e `peso_aluno` positivos.
* `meta_alfabetizacao_2030 ≥ meta_alfabetizacao_2024` (trajetória crescente).
* Valores decodificados de dicionário não podem ser nulos após o join.

---

## 8. Monitoramento da pipeline

A observabilidade proposta contempla:

* **falhas de ingestão**: taxa de erro por carga e origem;
* **latência**: tempo entre extração no BigQuery e persistência na Bronze;
* **volume**: linhas processadas por tabela e por execução;
* **qualidade**: percentual de nulos, duplicidades e quebras de chave;
* **alertas de erro**: notificações automáticas por falhas críticas de ingestão ou validação;
* **freshness**: data da última carga bem-sucedida por tabela;
* **custo**: armazenamento por camada e custo computacional por execução.

---

## 9. FinOps — Otimização de custos

### Como a arquitetura foi otimizada

* Persistência em **Delta/Parquet** com compressão colunar para minimizar armazenamento e leitura.
* `bronze_alunos` (maior volume) particionado por `ano`.
* Tabelas pequenas (`bronze_meta_brasil`) sem particionamento para evitar overhead.
* Cargas batch de alto volume (`alunos` histórico) isoladas das atualizações streaming.
* Camada Gold materializada apenas para os produtos analíticos efetivamente consumidos.
* `silver_dim_municipio` reutilizado como broadcast join para evitar shuffles desnecessários.

### Decisões que reduzem custos operacionais

* Streaming incremental delimitado por `(ano, rede)` para evitar reprocessamento total.
* Evitar recomputação na Gold: produtos analíticos são derivados de Silver já tratada.
* Separação clara entre camadas: consultas analíticas jamais acessam diretamente a Bronze.

### Estimativa de custo da arquitetura (referência Azure/Databricks)

| Componente | Estimativa |
| --- | --- |
| Armazenamento Bronze (Delta, compressão) | Baixo — dados brutos com particionamento por ano |
| Armazenamento Silver/Gold | Muito baixo — volume reduzido por agregação e deduplicação |
| Compute batch (Spark, cluster compartilhado) | Variável por frequência de carga |
| Compute streaming (incremental) | Baixo — janela limitada por (ano, rede) |
| BigQuery (leitura via Base dos Dados) | Custo por bytes lidos — minimizado com projeção de colunas |

---

## 10. Tecnologias utilizadas

| Tecnologia | Função | Justificativa |
| --- | --- | --- |
| Databricks (Azure) | Plataforma principal de engenharia e analytics | Suporte nativo a Spark, Delta Lake e Lakehouse; integração com Azure AD e storage; ambiente já disponível na organização |
| Apache Spark | Processamento distribuído (batch e streaming) | Escala horizontalmente, suporta ambos os modos de ingestão e integra diretamente com Delta Lake |
| Delta Lake | Formato de armazenamento analítico | ACID, versionamento, time travel e otimizações de leitura (Z-Order, liquid clustering) |
| BigQuery (Base dos Dados) | Fonte dos dados do INEP | Plataforma pública com dados educacionais brasileiros estruturados e dicionário centralizado |
| Databricks Notebooks | Desenvolvimento e modelagem | Permite desenvolvimento iterativo, documentação inline e execução interativa |
| Azure Blob Storage / ADLS Gen2 | Armazenamento do lakehouse | Custo baixo, alta durabilidade e integração nativa com Databricks |

---

## 11. Decisões arquiteturais

### Batch vs streaming
Batch para estabilidade, reprocessamento histórico e dados com baixa frequência de atualização. Streaming para capturar atualizações de metas e novos resultados com menor latência.

### Data lakehouse vs data warehouse isolado
O padrão lakehouse elimina silos entre engenharia de dados, exploração analítica e modelos de machine learning, reduzindo custo de infraestrutura e complexidade operacional.

### Custo vs performance
Materialização seletiva na Gold — apenas produtos com demanda real são computados. Evita varreduras amplas ao expor tabelas Bronze diretamente para BI.

---

## 12. Aplicação em IA

A camada Gold foi desenhada para suportar, futuramente:

* **modelos preditivos** de alfabetização por município com variáveis de metas, rede, série e proficiência;
* **análises de desigualdade** educacional por território e nível socioeconômico;
* **clusters de vulnerabilidade** com variáveis do Cadastro Único e Atlas IDH (via enriquecimento opcional);
* **priorização de políticas públicas** baseadas em evidências e gaps de meta vs. resultado.

---

## 13. Estrutura do repositório

```text
tech_challenge/
├── README.md
├── modelo_solucao.md
├── BigQuery INEP Avaliacao Alfabetizacao Extracao   ← blueprint notebook
├── pipelines/
│   ├── bronze/     ← scripts de ingestão batch e streaming
│   ├── silver/     ← transformações, deduplicação, dicionários
│   └── gold/       ← materialização dos produtos analíticos
├── quality/        ← scripts de validação e testes de qualidade
└── docs/           ← diagramas e documentação complementar
```

O repositório deve demonstrar o uso adequado de **Git** durante o desenvolvimento, incluindo:

* histórico de commits com mensagens claras e descritivas;
* uso de branches para desenvolvimento de funcionalidades (`feature/bronze-ingestion`, `feature/silver-transform`, etc.);
* criação de Pull Requests (PRs) para integração na branch principal (`main` ou `develop`);
* discussões ou comentários nas PRs que justifiquem as decisões técnicas realizadas.

---

## 14. Vídeo executivo

O grupo deverá gravar um vídeo de até **5 minutos** em linguagem executiva, cobrindo:

* problema de negócio (contexto da alfabetização e do Indicador Criança Alfabetizada);
* arquitetura da solução (pipeline híbrida e medalhão);
* valor da pipeline para análises educacionais;
* potencial uso para inteligência artificial e políticas públicas.

---

## 15. Conteúdo entregue nesta etapa

Nesta etapa foram produzidos apenas artefatos de modelagem:

* notebook com blueprint da solução e contratos de esquema por camada;
* documentação executiva e técnica (`README.md` e `modelo_solucao.md`);
* desenho lógico da pipeline com diagrama de fluxo;
* definição das camadas, regras de qualidade coluna a coluna e produtos analíticos.

Nenhum recurso foi criado ou provisionado.

---

## 16. Próximos passos

* conectar às fontes reais da Base dos Dados via BigQuery;
* implementar a ingestão batch das 6 tabelas;
* simular a ingestão streaming para metas e alunos;
* codificar regras de qualidade como testes automatizados;
* materializar produtos analíticos da camada Gold;
* configurar monitoramento e alertas operacionais;
* complementar o repositório com histórico Git, branches, PRs e evidências de execução.
