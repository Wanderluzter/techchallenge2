# Modelo da Solução

## 1. Visão geral

Este documento detalha o desenho lógico da solução implementada no Tech Challenge Fase 2: uma pipeline híbrida para análise da alfabetização no Brasil. A pipeline está funcional em Databricks/Azure, extraindo dados reais do BigQuery via `basedosdados.read_sql()` com decodificação integrada.

### Indicador Criança Alfabetizada

O **Compromisso Nacional Criança Alfabetizada** é uma política pública que mobiliza União, estados, Distrito Federal e municípios para garantir que todas as crianças brasileiras estejam alfabetizadas até o final do 2º ano do ensino fundamental.

Em 2023, o **INEP** realizou a **Pesquisa Alfabetiza Brasil**, definindo o corte de **743 pontos na escala de proficiência do Saeb** como referência para considerar uma criança alfabetizada. A partir desse parâmetro foi criado o **Indicador Criança Alfabetizada**, que expressa o percentual de estudantes que atingem esse patamar.

A **meta nacional** é que, até **2030**, todas as crianças brasileiras estejam alfabetizadas ao final do 2º ano do ensino fundamental.

---

## 2. Escopo funcional

### Fontes principais — `basedosdados.br_inep_avaliacao_alfabetizacao` (BigQuery)

| Entidade | Tabela BigQuery | Grain | Modo |
| --- | --- | --- | --- |
| UF | `uf` | ano, sigla_uf, serie, rede | Batch |
| Município | `municipio` | ano, id_municipio, serie, rede | Batch |
| Alunos | `alunos` | ano, id_aluno | Batch + Streaming |
| Meta Brasil | `meta_alfabetizacao_brasil` | ano, rede | Batch + Streaming |
| Meta por UF | `meta_alfabetizacao_uf` | ano, sigla_uf, rede | Batch + Streaming |
| Meta por Município | `meta_alfabetizacao_municipio` | ano, id_municipio, rede | Batch + Streaming |

Todas as colunas categóricas (`serie`, `rede`, `presenca`, `preenchimento_caderno`, `alfabetizado`) são decodificadas via LEFT JOIN com `br_inep_avaliacao_alfabetizacao.dicionario` na query SQL de extração (dados chegam já como texto ao Spark).

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

## 3. Padrão arquitetural

A arquitetura segue o padrão medalhão com ingestão híbrida:

### Bronze
Objetivo: preservar os dados brutos, com histórico e rastreabilidade.

Responsabilidades:

* recepção das tabelas BigQuery via batch e eventos streaming;
* armazenamento sem transformação relevante;
* auditoria de chegada, origem e timestamp de carga.

### Silver
Objetivo: padronizar, validar e integrar os dados.

Responsabilidades:

* decodificação de dicionários (`serie`, `rede`, `presenca`, `preenchimento_caderno`, `alfabetizado`);
* harmonização de tipos e normalização de chaves territoriais (`sigla_uf`, `id_municipio`);
* remoção de duplicatas por grain definido;
* tratamento de nulos em `proficiencia`, `taxa_alfabetizacao` e `media_portugues`;
* validação de integridade referencial município → UF;
* consolidação dos domínios de metas, território e desempenho.

### Gold
Objetivo: disponibilizar produtos analíticos de alto valor para BI, estatística e IA.

Responsabilidades:

* agregações e indicadores finais por recorte territorial e temporal;
* comparação entre metas (2024–2030) e resultados observados;
* séries históricas e perfis de proficiência;
* datasets prontos para dashboards, modelos preditivos e políticas públicas.

---

## 4. Estratégia híbrida

### Batch
Cargas periódicas para dados de baixa frequência de atualização:

* cadastros territoriais: `uf`, `municipio`;
* microdados históricos: `alunos` (carga completa por ano de avaliação);
* metas históricas: `meta_alfabetizacao_brasil`, `meta_alfabetizacao_uf`, `meta_alfabetizacao_municipio`.

### Streaming
Simulação de ingestão quase em tempo real para:

* novos resultados de avaliação: `alunos` (incremental por evento de medição);
* atualização de metas: `meta_alfabetizacao_brasil`, `meta_alfabetizacao_uf`, `meta_alfabetizacao_municipio`;
* revisão de indicadores regionais.

---

## 5. Contratos lógicos por camada

### Entradas esperadas na Bronze

| Entidade Bronze | Fonte BigQuery | Modo | Grain |
| --- | --- | --- | --- |
| bronze_uf | br_inep_avaliacao_alfabetizacao.uf | Batch | ano, sigla_uf, serie, rede |
| bronze_municipio | br_inep_avaliacao_alfabetizacao.municipio | Batch | ano, id_municipio, serie, rede |
| bronze_alunos | br_inep_avaliacao_alfabetizacao.alunos | Batch + Streaming | ano, id_aluno |
| bronze_meta_brasil | br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_brasil | Batch + Streaming | ano, rede |
| bronze_meta_uf | br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_uf | Batch + Streaming | ano, sigla_uf, rede |
| bronze_meta_municipio | br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_municipio | Batch + Streaming | ano, id_municipio, rede |

### Colunas por tabela Bronze

**bronze_uf / bronze_municipio**
`ano`, `sigla_uf` / `id_municipio`, `sigla_uf_nome` / `id_municipio_nome`, `serie`, `rede`, `taxa_alfabetizacao`, `media_portugues`, `proporcao_aluno_nivel_0..8`

> Dicionário aplicado nas colunas `serie` e `rede` via join com `br_inep_avaliacao_alfabetizacao.dicionario`.

**bronze_alunos**
`ano`, `id_municipio`, `id_municipio_nome`, `id_escola`, `id_aluno`, `caderno`, `serie`, `rede`, `presenca`, `preenchimento_caderno`, `alfabetizado`, `proficiencia`, `peso_aluno`

> Dicionário aplicado nas colunas `serie`, `rede`, `presenca`, `preenchimento_caderno` e `alfabetizado`.

**bronze_meta_brasil**
`ano`, `rede`, `taxa_alfabetizacao`, `meta_alfabetizacao_2024..2030`, `percentual_participacao`

**bronze_meta_uf**
`ano`, `sigla_uf`, `sigla_uf_nome`, `rede`, `taxa_alfabetizacao`, `meta_alfabetizacao_2024..2030`, `percentual_participacao`

**bronze_meta_municipio**
`ano`, `id_municipio`, `id_municipio_nome`, `rede`, `taxa_alfabetizacao`, `meta_alfabetizacao_2024..2030`, `nivel_alfabetizacao`, `percentual_participacao`

### Saídas esperadas na Silver

| Entidade Silver | Fonte | Transformações principais |
| --- | --- | --- |
| silver_dim_uf | bronze_uf + bronze_meta_uf | Normalizar sigla_uf, garantir unicidade |
| silver_dim_municipio | bronze_municipio + bronze_meta_municipio + bronze_alunos | Normalizar id_municipio, validar vínculo com UF |
| silver_indicadores_uf | bronze_uf | Decodificar dicionário, validar faixas de proporção |
| silver_indicadores_municipio | bronze_municipio | Decodificar dicionário, validar faixas, cruzar com dim_municipio |
| silver_microdados_alunos | bronze_alunos | Decodificar 5 colunas de dicionário, tratar nulos, remover duplicatas |
| silver_meta_brasil | bronze_meta_brasil | Validar unicidade por (ano, rede), verificar completude das metas |
| silver_meta_uf | bronze_meta_uf | Validar sigla_uf, cruzar consistência com meta_brasil |
| silver_meta_municipio | bronze_meta_municipio | Validar id_municipio, verificar nivel_alfabetizacao |

### Produtos esperados na Gold

| Produto Gold | Fonte | Grain | Finalidade |
| --- | --- | --- | --- |
| gold_indicador_municipio | silver_indicadores_municipio + silver_dim_municipio | ano, id_municipio, rede, serie | Dashboards regionais e rankings |
| gold_gap_meta_resultado | silver_meta_municipio + silver_indicadores_municipio | ano, id_municipio, rede | Monitoramento de metas e progresso |
| gold_evolucao_temporal | silver_indicadores_uf + silver_indicadores_municipio | ano, sigla_uf, rede, serie | Série histórica e modelos preditivos |
| gold_perfil_nivel_proficiencia | silver_microdados_alunos + silver_dim_municipio | ano, id_municipio, rede, serie | Distribuição de proficiência e clusters |
| gold_painel_executivo | gold_indicador_municipio + gold_gap_meta_resultado + silver_meta_brasil | ano, rede | Visão executiva e políticas públicas |

---

## 6. Regras de qualidade de dados

Baseadas nos esquemas reais das 6 tabelas da Base dos Dados.

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

* `sigla_uf` não pode ser nula em `bronze_uf` e `bronze_meta_uf`.
* `id_municipio` não pode ser nulo em `bronze_municipio`, `bronze_meta_municipio` e `bronze_alunos`.
* `id_aluno` não pode ser nulo em `bronze_alunos`.
* `ano` obrigatório em todas as tabelas.
* `rede` obrigatória nas tabelas de metas.
* `taxa_alfabetizacao` obrigatória nas tabelas de indicadores e metas.
* `proficiencia` obrigatória em `bronze_alunos` salvo alunos ausentes.
* Colunas `meta_alfabetizacao_2024..2030` obrigatórias nas tabelas de meta.

### Integridade referencial

* `sigla_uf` em `bronze_uf` e `bronze_meta_uf` deve existir em `br_bd_diretorios_brasil.uf`.
* `id_municipio` em `bronze_municipio`, `bronze_meta_municipio` e `bronze_alunos` deve existir em `br_bd_diretorios_brasil.municipio`.
* Metas municipais devem ter `id_municipio` presente em `silver_dim_municipio`.

### Consistência

* `proporcao_aluno_nivel_0..8`: cada valor entre 0 e 100.
* Soma das `proporcao_nivel` deve ser ≈ 100 por linha (tolerância de 1%).
* `taxa_alfabetizacao` entre 0 e 100.
* `proficiencia` e `peso_aluno` positivos em `bronze_alunos`.
* `meta_alfabetizacao_2030 >= meta_alfabetizacao_2024` (trajetória crescente esperada).
* `percentual_participacao` entre 0 e 100.
* Valores decodificados de `serie`, `rede`, `presenca`, `preenchimento_caderno` e `alfabetizado` não podem ser nulos após join com dicionário.

---

## 7. Monitoramento da pipeline

### Indicadores operacionais

* **falhas de ingestão**: taxa de erro por carga e origem (BigQuery → Bronze);
* **latência**: tempo entre extração no BigQuery e persistência na Bronze;
* **volume**: linhas processadas por tabela e por execução;
* **qualidade**: percentual de nulos, duplicidades e quebras de chave por camada;
* **alertas de erro**: notificações automáticas por falhas críticas de ingestão ou validação;
* **freshness**: data da última carga bem-sucedida por tabela;
* **custo**: armazenamento por camada e custo computacional por execução.

---

## 8. FinOps — Otimização de custos

### Como a arquitetura foi otimizada

* Persistência em **Delta/Parquet** com compressão colunar para minimizar armazenamento e leitura.
* `bronze_alunos` (maior volume) particionado por `ano`.
* Tabelas pequenas (`bronze_meta_brasil`) sem particionamento para evitar overhead.
* Cargas batch de alto volume (`alunos` histórico) isoladas das atualizações streaming.
* Camada Gold materializada apenas para os produtos analíticos efetivamente consumidos.
* `silver_dim_municipio` reutilizado como broadcast join para evitar shuffles desnecessários.

### Decisões que reduzem custos operacionais

* Streaming incremental delimitado por `(ano, rede)` para evitar reprocessamento total.
* Evitar recomputação na Gold: produtos são derivados de Silver já tratada.
* Consultas analíticas jamais acessam diretamente a Bronze.

### Estimativa de custo da arquitetura (referência Azure/Databricks)

| Componente | Estimativa |
| --- | --- |
| Armazenamento Bronze (Delta, compressão) | Baixo — dados brutos com particionamento por ano |
| Armazenamento Silver/Gold | Muito baixo — volume reduzido por agregação e deduplicação |
| Compute batch (Spark, cluster compartilhado) | Variável por frequência de carga |
| Compute streaming (incremental) | Baixo — janela limitada por (ano, rede) |
| BigQuery (leitura via Base dos Dados) | Custo por bytes lidos — minimizado com projeção de colunas |

---

## 9. Tecnologias utilizadas

| Tecnologia | Função | Justificativa |
| --- | --- | --- |
| Databricks (Azure) | Plataforma principal de engenharia e analytics | Suporte nativo a Spark, Delta Lake e Lakehouse; integração com Azure AD e storage |
| Apache Spark | Processamento distribuído (batch e streaming) | Escala horizontal, suporta ambos os modos de ingestão e integra com Delta Lake |
| Delta Lake | Formato de armazenamento analítico | ACID, versionamento, time travel e otimizações de leitura (Z-Order, liquid clustering) |
| BigQuery (Base dos Dados) | Fonte dos dados do INEP | Plataforma pública com dados educacionais brasileiros estruturados e dicionário centralizado |
| Databricks Notebooks | Desenvolvimento e modelagem | Desenvolvimento iterativo, documentação inline e execução interativa |
| Azure Blob Storage / ADLS Gen2 | Armazenamento do lakehouse | Custo baixo, alta durabilidade e integração nativa com Databricks |

---

## 10. Decisões arquiteturais

### Batch vs streaming
Batch para estabilidade, reprocessamento histórico e dados com baixa frequência de atualização. Streaming para capturar atualizações de metas e novos resultados com menor latência.

### Data lakehouse vs data warehouse isolado
O padrão lakehouse elimina silos entre engenharia de dados, exploração analítica e modelos de machine learning, reduzindo custo de infraestrutura e complexidade operacional.

### Custo vs performance
Materialização seletiva na Gold — apenas produtos com demanda real são computados. Evita varreduras amplas ao expor tabelas Bronze diretamente para BI.

---

## 11. Aplicação em IA

A camada Gold foi desenhada para suportar, futuramente:

* **modelos preditivos** de alfabetização por município com variáveis de metas, rede, série e proficiência;
* **análises de desigualdade** educacional por território e nível socioeconômico;
* **clusters de vulnerabilidade** com variáveis do Cadastro Único e Atlas IDH (via enriquecimento opcional);
* **priorização de políticas públicas** baseadas em evidências e gaps de meta vs. resultado.

---

## 12. Diretrizes de Git

O repositório deve demonstrar uso adequado de **Git** durante o desenvolvimento, incluindo:

* histórico de commits com mensagens claras e descritivas;
* uso de branches por funcionalidade (`feature/bronze-ingestion`, `feature/silver-transform`, `feature/gold-products`, etc.);
* criação de Pull Requests (PRs) para integração na branch principal (`main` ou `develop`);
* discussões ou comentários nas PRs que justifiquem as decisões técnicas realizadas.

---

## 13. Vídeo executivo

O grupo deverá gravar um vídeo de até **5 minutos** em linguagem executiva, cobrindo:

* problema de negócio (contexto da alfabetização e do Indicador Criança Alfabetizada);
* arquitetura da solução (pipeline híbrida e medalhão);
* valor da pipeline para análises educacionais;
* potencial uso para inteligência artificial e políticas públicas.

---

## 14. Resultado desta etapa

Foram definidos:

* blueprint do notebook com contratos de esquema por camada;
* documentação executiva e técnica (`README.md` e `modelo_solucao.md`);
* desenho lógico da pipeline com diagrama de fluxo;
* regras de qualidade coluna a coluna e produtos analíticos.

Nenhum recurso de infraestrutura foi criado nesta fase.
