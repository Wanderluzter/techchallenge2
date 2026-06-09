# 📚 Tech Challenge: Pipeline Híbrido - Análise da Alfabetização no Brasil

## 👥 Equipe
* Leonardo Botelho 370654
* Rafael Silva 365112

---

## 1. 🎯 Contexto do Problema e Negócio
A alfabetização na infância é um pilar para o desenvolvimento do Brasil. Com o **Compromisso Nacional Criança Alfabetizada**, a meta é que todas as crianças estejam alfabetizadas até o 2º ano do ensino fundamental até 2030 (corte de 743 pontos no Saeb).

Este projeto visa solucionar o desafio de integrar dados educacionais dispersos, criando uma **pipeline de dados híbrida e escalável** que unifica metas nacionais, estaduais e dados territoriais. Essa infraestrutura permite análises profundas sobre a desigualdade educacional e apoia a criação de políticas públicas baseadas em evidências.

---

## 2. 🏗️ Arquitetura da Solução

*(imagem do diagrama da sua arquitetura)*
`![Diagrama da Arquitetura](link_para_imagem)`

### Fluxo de Dados:
1. **Fontes:** Extração de dados da plataforma *Base dos Dados* (Batch) e simulação de eventos em tempo real (Streaming).
2. **Ingestão:** Os dados são capturados via `[Ferramenta escolhida]` e armazenados na nuvem `[AWS/GCP/Azure]`.
3. **Processamento:** Aplicação da Arquitetura Medalhão (detalhes na seção 4).
4. **Consumo:** Disponibilização da camada analítica para Dashboards e Machine Learning.

---

## 3. 🛠️ Tecnologias Utilizadas

| Componente | Tecnologia | Justificativa |
| :--- | :--- | :--- |
| **Cloud Provider** | `[Ex: AWS]` | `[Ex: Familiaridade da equipe e ecossistema integrado]` |
| **Ingestão Batch** | `[Ex: Python/Airflow]`| `[Ex: Agendamento flexível e fácil manutenção]` |
| **Ingestão Streaming**| `[Ex: Apache Kafka]` | `[Ex: Alta capacidade de vazão para eventos em tempo real]` |
| **Processamento** | `[Ex: Apache Spark]` | `[Ex: Processamento distribuído escalável para grandes volumes]` |
| **Storage (Data Lake)**| `[Ex: Amazon S3]` | `[Ex: Baixo custo e suporte nativo a versionamento e Parquet]` |

---

## 4. 🥇 Arquitetura Medalhão

Nossa pipeline está dividida em três camadas principais:

* **🥉 Bronze (Raw Data):** Armazenamento dos dados brutos exatamente como extraídos da origem. Histórico completo mantido.
* **🥈 Silver (Trusted Data):** Dados limpos e padronizados. Nesta camada realizamos:
    * Remoção de duplicatas.
    * Tratamento de valores nulos.
    * Tipagem correta de colunas (datas, strings, numéricos).
    * Integração das tabelas de Metas com a tabela de Municípios.
* **🥇 Gold (Analytical Data):** Datasets prontos para consumo. Entregáveis:
    * `tb_indicador_alfabetizacao_municipio`: Visão consolidada por cidade.
    * `tb_comparativo_metas_resultados`: Cruzamento temporal.

---

## 5. ⚖️ Qualidade de Dados (Data Quality)

Para garantir a confiabilidade da pipeline, implementamos validações automáticas na passagem da Bronze para a Silver:
* Verificação de duplicidade nas chaves primárias (`id_municipio`).
* Alertas para percentual alto de valores ausentes.
* Validação de consistência relacional (Municípios órfãos de UF).

---

## 6. 💰 FinOps e Monitoramento

### FinOps (Otimização de Custos)
As seguintes decisões arquiteturais foram tomadas para reduzir custos operacionais:
* **Formato Colunar:** Uso exclusivo de `.parquet` nas camadas Silver e Gold para reduzir espaço e baratear queries.
* **Particionamento:** Os dados Gold estão particionados por `Ano` e `UF`, reduzindo a quantidade de dados escaneados em consultas analíticas.
* **Recursos Efêmeros:** O cluster de processamento sobe apenas durante a carga Batch e é desligado automaticamente.

### Monitoramento e Observabilidade
* O orquestrador envia logs de *Sucesso/Falha* para `[Ferramenta, ex: Slack / CloudWatch]`.
* Monitoramos a latência da fila de streaming e o volume de dados diário processado.

---

## 7. ⚖️ Decisões Arquiteturais (Trade-offs)

* **Batch vs Streaming:** Optamos por manter os dados demográficos e metas em Batch (mudam pouco), reservando o Streaming apenas para a medição de desempenho contínua, economizando infraestrutura de tempo real.
* **Data Lake vs Data Warehouse:** Escolhemos uma abordagem Data Lakehouse `[Ex: S3 + Athena]`. Isso nos dá a flexibilidade e o baixo custo do Data Lake, com a performance de query de um DW para a camada Gold.

---

## 8. 🤖 Potencial de Aplicação em Inteligência Artificial

A base **Gold** construída estabelece o alicerce para análises avançadas:
1. **Predição de Desempenho:** Utilizar as taxas de alfabetização e indicadores socioeconômicos cruzados para prever quais municípios têm maior risco de não bater a meta até 2030.
2. **Análise de Clusters:** Agrupar municípios com perfis semelhantes de vulnerabilidade educacional para direcionamento específico de verbas do FUNDEB.
3. **LLMs para Gestores Públicos:** Integrar a base Gold a um modelo de linguagem (RAG) para que prefeitos e secretários possam fazer perguntas em texto natural (ex: *"Qual a evolução da alfabetização na minha cidade em relação ao meu estado?"*).

---

## 9. ⚙️ Como Executar o Projeto

1. Clone o repositório: `git clone [url_do_repo]`
2. Instale as dependências: `pip install -r requirements.txt`
3. Configure as variáveis de ambiente no arquivo `.env` (credenciais da Cloud).
4. Execute o script de provisionamento: `[comando, ex: terraform apply ou bash setup.sh]`
5. Para rodar a pipeline localmente: `[comando de execução principal]`

---

## 10. 🎬 Apresentação Executiva

Acesse nossa apresentação focada no valor de negócio e estratégia arquitetural:

🎥 **[LINK PARA O VÍDEO NO YOUTUBE / DRIVE AQUI]**
