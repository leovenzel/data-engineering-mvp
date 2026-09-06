# data-engineering-mvp
MVP de Engenharia de Dados - Pipeline de Aluguel por Temporada no RJ (Databricks / Lakehouse).

# MVP de Engenharia de Dados - Análise de Aluguel por Temporada (Inside Airbnb RJ)

## 1. Visão Geral do Projeto
Este projeto consiste no desenvolvimento de um pipeline de dados de ponta a ponta construído na plataforma **Databricks** (Unity Catalog), utilizando a arquitetura **Medalhão (Bronze, Silver, Gold)** e **PySpark/SQL**. 

O objetivo principal é extrair, tratar, modelar e analisar dados de anúncios de aluguel por temporada no Rio de Janeiro para apoiar tomadas de decisão estratégicas do negócio.

---

## 2. Arquitetura da Solução (Pipeline Medalhão)

A solução foi estruturada no Databricks em três camadas contínuas:

* **Camada Bronze (`01_ingestion_bronze`):** Ingestão do arquivo bruto `listings.csv.gz` do Inside Airbnb armazenado em um Volume do Unity Catalog (`/Volumes/workspace/default/raw_data/`). Persistência no formato Delta Lake sem alterações estruturais nos dados nativos, adicionando metadados de auditoria (`_ingestion_timestamp` e `_source_file`).
* **Camada Silver (`02_transformation_silver`):** Limpeza, tipagem e enriquecimento de dados. Redução de 75 colunas brutas para 23 colunas relevantes. Tratamento de valores monetários (conversão de texto com cifrão para `DECIMAL(10,2)`), parsing de comodidades (Wi-Fi, Ar-Condicionado, Piscina) e agrupamento geográfico por Zonas do Rio de Janeiro.
* **Camada Gold (`03_modeling_gold`):** Construção da modelagem dimensional **Star Schema** (Tabela Fato e Dimensões) e catalogação de metadados via SQL no Unity Catalog.
* **Camada Analytics (`04_analytics_insights`):** Execução de consultas SQL/PySpark para responder às perguntas de negócio.

---

## 3. Qualidade e Governança de Dados

### 3.1. Estratégia de Filtragem e Limpeza
* **Redução de Dimensão:** Redução das 75 colunas nativas para 23 colunas úteis (~69% de otimização de largura), descartando colunas de texto livre e URLs que trariam custo excessivo de processamento.
* **Integridade Operacional:** Preservação de 100% das linhas tratadas na camada Silver/Gold.
* **Tratamento Financeiro:** Limpeza via Expressões Regulares (`regexp_replace`) para converter strings financeiras em tipos numéricos precisos.
* **Classificação Territorial:** Mapeamento condicional (`when/otherwise`) dos bairros da coluna `neighbourhood_cleansed` nas Zonas Sul, Norte, Oeste, Centro e Outros.

---

## 4. Modelagem Dimensional (Star Schema) & Catálogo de Dados

O modelo dimensional é composto por 3 Tabelas Dimensão e 1 Tabela Fato:

### 4.1. Tabela Fato: `fact_listings`
| Coluna | Tipo de Dado | Descrição | Regra / Chave |
| :--- | :--- | :--- | :--- |
| `property_id` | BIGINT | Identificador único do imóvel anúncio | FK -> `dim_property` |
| `host_id` | BIGINT | Identificador do anfitrião | FK -> `dim_host` |
| `location_id` | INT | Identificador da localização | FK -> `dim_location` |
| `price` | DECIMAL(10,2) | Valor da diária em Reais (BRL) | Métrica |
| `minimum_nights` | INT | Mínimo de noites exigido | Métrica |
| `maximum_nights` | INT | Máximo de noites permitido | Métrica |
| `number_of_reviews` | INT | Total de avaliações recebidas | Métrica |
| `review_score` | DOUBLE | Nota média geral (0 a 5) | Métrica |
| `latitude` | DOUBLE | Coordenada de latitude | Atributo |
| `longitude` | DOUBLE | Coordenada de longitude | Atributo |
| `_created_at` | TIMESTAMP | Data/hora de processamento | Metadado |

### 4.2. Tabelas Dimensão
* **`dim_host`**: `host_id` (PK), `host_name`, `host_since`, `is_superhost` (BOOLEAN), `host_listings_count`.
* **`dim_location`**: `location_id` (PK), `neighbourhood`, `zone`.
* **`dim_property`**: `property_id` (PK), `property_type`, `room_type`, `accommodates`, `bedrooms`, `beds`, `has_wifi`, `has_air_conditioning`, `has_pool`, `has_sea_view`.

---

## 5. Respostas às Perguntas de Negócio (Insights)

1. **Preço por Zona Geográfica:** A Zona Sul e a Zona Oeste concentram as maiores medianas de diárias do município do Rio de Janeiro, impulsionadas pela proximidade da orla marítima e perfil dos imóveis.
2. **Impacto de Comodidades:** Imóveis que combinam **Ar-Condicionado** e **Vista para o Mar** registram ticket médio e mediano significativamente superiores em comparação a acomodações básicas.
3. **Desempenho de Superhosts:** Anfitriões com selo *Superhost* apresentam médias de avaliação superiores e mantêm precificação competitiva com alta taxa de ocupação reputacional.
4. **Perfil do Mercado:** Observa-se relevante presença de anfitriões profissionais (multi-proprietários com >1 imóvel), controlando parcela expressiva dos anúncios ativos.
5. **Regra de Estadia:** Anúncios focados em estadias curtas (1-2 noites) possuem diárias com preço mediano mais elevado do que anúncios com exigência de estadias longas.

---

## 6. Autoavaliação e Trabalhos Futuros (MVP 2.0)

### Pontos Fortes do Projeto
* Implementação rigorosa do pipeline Medalhão com separação de responsabilidades em notebooks modulares.
* Rastreabilidade e governança via Unity Catalog com comentários formais em todas as colunas.
* Código 100% versionado via Git/GitHub utilizando boas práticas de commit.

### Limitações e Recomendações para Evolução (MVP 2.0)
* **Temporalidade da Base:** O projeto utilizou um *snapshot* estático do Inside Airbnb. Como proposta de melhoria para o MVP 2.0, recomenda-se a ingestão da série histórica (múltiplos snapshots trimestrais) ou integração da tabela `calendar.csv.gz` para análise de sazonalidade e taxa de ocupação diária.
* **Automação de Pipelines:** Implementação de orquestração automatizada via Databricks Workflows / Jobs com alertas de falha.