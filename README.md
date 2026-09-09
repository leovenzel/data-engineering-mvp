# MVP de Engenharia de Dados - Pipeline Medalhão & Governança no Databricks (Inside Airbnb RJ)

## 1. Contexto de Negócio e Motivação

### 1.1. Conexão e Evolução em Relação ao MVP de Machine Learning
No projeto anterior, focado no desenvolvimento de um modelo preditivo para estimativa de diárias por temporada no Rio de Janeiro (via *Gradient Boosting Regressor*), identificou-se uma limitação crítica nos dados consumidos: o **Underfitting Estrutural por Ausência de Dados Qualitativos**. O modelo de ML ficou restrito a variáveis quantitativas e físicas (como número de quartos e banheiros), atingindo um teto de aprendizado ($R^2 = 0.3478$). Naquele relatório, apontou-se como evolução indispensável a estruturação de dados qualitativos e reputacionais (comodidades como ar-condicionado, piscina, vista para o mar, notas de avaliação e selo de *Superhost*).

Este MVP de Engenharia de Dados **não busca retreinar ou alterar o modelo preditivo anterior**. Seu propósito é construir a infraestrutura moderna de Lakehouse que viabilize, padronize e facilite análises e modelagens futuras. Através da arquitetura Medalhão no Databricks, este trabalho atua como a camada fundacional de Engenharia, transformando dados brutos e desestruturados em um catálogo governado e modelado.

### 1.2. Objetivo do Trabalho
O objetivo principal é projetar, implementar e validar um **pipeline de dados de ponta a ponta (End-to-End Data Pipeline)** na plataforma **Databricks**, seguindo a **Arquitetura Medalhão (Camadas Bronze, Silver e Gold)** sob a governança do **Unity Catalog**. 

O projeto resolve a complexidade de ingestão, higienização, tipagem e modelagem dimensional de dados não estruturados de aluguel por temporada, entregando um **Star Schema (Esquema Estrela)** documentado que responde às dúvidas estratégicas do negócio de forma automatizada e reprodutível.

---

## 2. Contexto dos Dados Brutos e Licenciamento

### 2.1. Fonte de Dados e Licença
Os dados são oriundos da plataforma aberta **Inside Airbnb**, projeto independente que disponibiliza dados públicos extraídos (*scraped*) do site do Airbnb para fins de análise e transparência urbana.

* **Fonte Original:** [Inside Airbnb - Rio de Janeiro Dataset](http://insideairbnb.com/get-the-data.html)
* **Licenciamento:** Disponibilizados sob a licença **Creative Commons Attribution 4.0 International (CC BY 4.0)**, permitindo uso, compartilhamento e adaptação mediante citação da fonte.
* **Data de Coleta (*Snapshot*):** A base consumida refere-se a uma foto estática extraída na varredura do dia **24 de junho de 2026**.

### 2.2. Volumetria e Estrutura dos Dados Brutos
* **Volumetria Exata:** O arquivo bruto `listings.csv.gz` contém exatamente **48.713 registros (anúncios)** e **90 colunas nativas** abrangendo o município do Rio de Janeiro.
* **Complexidade Estrutural:** Combinação de textos livres desestruturados (`description`), URLs de mídia, listas de comodidades codificadas em string (`amenities`), valores monetários formatados como texto (`"$1,200.00"`), datas, variáveis reputacionais e coordenadas geográficas.
* **Tratamento de Ingestão:** O arquivo apresenta quebras de linha internas nos campos de texto (`\n`), exigindo configurações específicas de *parsing* CSV multilinha na camada Bronze para evitar corrupção na contagem de registros.

---

## 3. Formulação das Perguntas de Negócio

Para direcionar as transformações da Camada Silver e a modelagem dimensional da Camada Gold, foram estabelecidas **5 Perguntas de Negócio**:

1. **Variação Territorial de Preços:** Qual é o preço médio e mediano das diárias praticadas nos imóveis em cada Zona Geográfica do Rio de Janeiro (Zona Sul, Zona Norte, Zona Oeste, Centro e Outros)?
2. **Valoração de Comodidades Críticas:** Qual é o impacto financeiro na mediana de preço das diárias ao comparar imóveis que possuem comodidades de alto valor percebido (como Ar-Condicionado e Vista para o Mar) versus imóveis básicos?
3. **Análise Reputacional (Superhosts):** Anfitriões detentores do selo *Superhost* praticam preços superiores e possuem notas médias de avaliação mais altas em relação aos anfitriões comuns?
4. **Perfil de Oferta e Profissionalização:** Qual é a distribuição do mercado entre anfitriões individuais (amadores) e multi-proprietários (profissionais com mais de 1 imóvel), e como o preço varia entre esses perfis?
5. **Regras Operacionais e Precificação:** Como a exigência do número mínimo de noites de reserva (estadias curtas de 1-2 noites vs. estadias médias/longas) se relaciona com o valor da diária?

---

## 4. Carga e Ingestão de Dados (Camada Bronze)

### 4.1. Estratégia de Coleta e Armazenamento Bruto
A etapa de coleta e ingestão dos dados (*Data Ingestion*) foi projetada para garantir a rastreabilidade e a reprodutibilidade integral da fonte original sem modificar as características nativas do conjunto de dados:

* **Mecanismo de Download e Armazenamento:** O arquivo compactado `listings.csv.gz` (contendo os **48.713 registros** e **90 colunas nativas** do *snapshot* de 24/06/2026) foi importado diretamente para o ambiente de nuvem do Databricks e armazenado em um **Volume do Unity Catalog** no caminho gerenciado:
  `/Volumes/workspace/default/raw_data/listings.csv.gz`
* **Benefício de Governança:** O uso de Volumes no Unity Catalog permite que a camada bruta permaneça isolada, segura e acessível via controle de acesso baseado em papéis (RBAC), prevenindo alterações acidentais na fonte de dados nativa.

![Volume raw_data no Unity Catalog](./docs/01_volume_raw_data.png)

*Figura 1: Arquivo bruto listings.csv.gz armazenado no Volume raw_data do Unity Catalog.*

### 4.2. Execução Técnica do Pipeline de Ingestão (`01_ingestion_bronze`)
A carga dos dados brutos para o Data Lakehouse foi automatizada através do script PySpark desenvolvido no notebook **`01_ingestion_bronze`**, localizado na raiz do repositório no GitHub.

* **Tratamento de Desafios da Fonte Bruta:** Devido ao fato de as descrições dos imóveis conterem quebras de linha (`\n`) e caracteres especiais (como vírgulas e aspas internas), o script de ingestão configurou parâmetros específicos no leitor do Spark para evitar corrupção na contagem de linhas:
  * `multiline=True`: Permite que o parser interprete textos longos que se estendem por múltiplas linhas sem criar registros inválidos.
  * `quote='"'` e `escape='"'`: Garantem a correta identificação dos delimitadores de texto.
  * `inferSchema=True`: Permite a inferência inicial dos tipos de dados para auditoria.

* **Persistência em Tabela Delta Lake (`bronze_listings`):**
  Os dados brutos foram salvos na tabela gerenciada `workspace.default.bronze_listings` utilizando o formato aberto **Delta Lake**. Para garantir a rastreabilidade e auditoria da ingestão, foram injetadas duas colunas de metadados operacionais:
  * `_ingestion_timestamp`: Data e hora exatas da execução do pipeline.
  * `_source_file`: Identificador do arquivo de origem.

```python
# Trecho do script PySpark de Ingestão (01_ingestion_bronze)
raw_path = "/Volumes/workspace/default/raw_data/listings.csv.gz"

df_raw = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("multiline", "true") \
    .option("quote", '"') \
    .option("escape", '"') \
    .load(raw_path)

# Adição de Metadados de Auditoria e Persistência na Camada Bronze
from pyspark.sql.functions import current_timestamp, lit

df_bronze = df_raw \
    .withColumn("_ingestion_timestamp", current_timestamp()) \
    .withColumn("_source_file", lit("listings.csv.gz"))

df_bronze.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.default.bronze_listings")

```

* **Referência ao Código Fonte:** O código completo e executável desta etapa encontra-se versionado no repositório no arquivo [`01_ingestion_bronze.ipynb`](https://www.google.com/search?q=./01_ingestion_bronze).

![Amostra da Tabela Bronze](./docs/02_bronze_table_sample.png)

*Figura 2: Registros brutos persistidos na tabela Delta bronze_listings com colunas de auditoria.*

## 5. Arquitetura do Pipeline de Dados ETL (`Etapa 4.4`)

### 5.1. Organização e Modularização do Pipeline
Para garantir manutenibilidade, reuso de código, isolamento de falhas e auditoria em ambiente produtivo, o processo de ETL/ELT não foi concentrado em um único script monolítico. O pipeline foi estrategicamente desacoplado em **4 notebooks especializados**, executados de forma sequencial e alinhados às etapas da Arquitetura Medalhão:

1. **`01_ingestion_bronze.ipynb` (Camada Bronze):** Automação da coleta e ingestão da fonte bruta (`listings.csv.gz`) armazenada no Volume do Unity Catalog, persistindo a tabela `bronze_listings` com injeção de colunas de auditoria (`_ingestion_timestamp` e `_source_file`).
2. **`02_transformation_silver.ipynb` (Camada Silver):** Execução da limpeza pesada, filtros geográficos do município do Rio de Janeiro, deduplicação, sanitização de caracteres monetários via expressão regular, *casting* de tipos de dados e *parsing* do campo desestruturado `amenities`.
3. **`03_modeling_gold.ipynb` (Camada Gold):** Implementação da modelagem dimensional em **Star Schema**, dividindo os dados purificados em **1 Tabela Fato** (`fact_listings`) e **3 Tabelas Dimensão** (`dim_host`, `dim_location` e `dim_property`).
4. **`04_analytics_insights.ipynb` (Camada Analytics):** Execução de consultas analíticas em Spark SQL / PySpark agregando métricas para responder de forma quantitativa às 5 Perguntas de Negócio formuladas na Etapa 4.1.

### 5.2. Governança, Persistência na Nuvem e Repositório
Todas as tabelas do pipeline são salvas e governadas nativamente na nuvem através do metastore do **Unity Catalog** sob o schema `workspace.default`. A persistência utiliza o formato aberto **Delta Lake**, garantindo suporte a transações ACID, otimização de leitura e versionamento histórico dos dados (*Time Travel*).

```python
# Mapeamento do fluxo de persistência no Lakehouse (Unity Catalog)
# Bronze  -> workspace.default.bronze_listings
# Silver  -> workspace.default.silver_listings
# Gold    -> workspace.default.fact_listings (Fato)
#            workspace.default.dim_host (Dimensão)
#            workspace.default.dim_location (Dimensão)
#            workspace.default.dim_property (Dimensão)

```

* **Referência aos Scripts no GitHub:** Os notebooks executáveis do pipeline encontram-se versionados na raiz do repositório:
  * [`01_ingestion_bronze.ipynb`](./01_ingestion_bronze.ipynb)
  * [`02_transformation_silver.ipynb`](./02_transformation_silver.ipynb)
  * [`03_modeling_gold.ipynb`](./03_modeling_gold.ipynb)
  * [`04_analytics_insights.ipynb`](./04_analytics_insights.ipynb)

![Tabelas Persistidas no Unity Catalog](./docs/03_pipeline_tables_catalog.png)

*Figura 3: Visão geral do Catalog Explorer no schema default evidenciando a persistência física e governança de todas as tabelas do pipeline (Bronze, Silver e Gold) no Unity Catalog.*
