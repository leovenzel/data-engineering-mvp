# data-engineering-mvp
MVP de Engenharia de Dados - Pipeline de Aluguel por Temporada no RJ (Databricks / Lakehouse).

# MVP de Engenharia de Dados - Pipeline de Aluguel por Temporada no Rio de Janeiro (Inside Airbnb)

## Contexto de Negócio e Perguntas (Etapa 2 e 4.1)

### Contexto do Problema
No desenvolvimento do MVP de Machine Learning anterior, focado na precificação inteligente de diárias de imóveis por temporada no Rio de Janeiro, foi desenvolvido um modelo preditivo baseado em *Gradient Boosting Regressor*. Embora o modelo tenha alcançado um ganho de performance de 21,96% sobre a mediana de mercado, foi diagnosticado um **underfitting estrutural por ausência de dados qualitativos e geográficos ricos**, estabelecendo um teto estatístico no Coeficiente de Determinação ($R^2 = 34,78\%$). 

A base nativa utilizada naquele momento continha apenas variáveis estruturais básicas (quantidade de quartos, banheiros, noites mínimas e localização genérica) coletadas em um recorte curto de baixa temporada. Na prática do mercado imobiliário carioca, atributos qualitativos (presença de ar-condicionado, piscina, Wi-Fi, vista para o mar), o perfil reputacional do anfitrião (*Superhost*, notas de avaliação) e o detalhamento socioespacial (divisão por zonas e bairros) exercem forte impacto na precificação e na valorização das propriedades.

O objetivo deste projeto de Engenharia de Dados é construir, do zero, um pipeline robusto, escalável e automatizado na plataforma **Databricks (Lakehouse)** utilizando a **Arquitetura Medalhão (Bronze, Silver e Gold)**. Este pipeline irá ingerir, tratar, modelar e catalogar a base de dados pública e completa do **Inside Airbnb Rio de Janeiro**, disponibilizando um ambiente analítico confiável para responder a perguntas estratégicas de negócio e pavimentar a infraestrutura necessária para alimentar futuros modelos preditivos enriquecidos.

---

### Perguntas de Negócio
Para guiar todas as etapas de ingestão, limpeza, modelagem e análise, foram formuladas 5 perguntas de negócio imutáveis que buscam isolar e mensurar o impacto dos fatores qualitativos e geográficos que faltavam no projeto anterior:

1. **Impacto Socioespacial (Geografia):** Qual é a disparidade na diária média e mediana entre os bairros da Zona Sul (ex.: Copacabana, Ipanema, Leblon) em comparação com as Zonas Norte, Oeste e Centro do Rio de Janeiro?
2. **Valorização por Comodidades Premium (Subjetividade):** De que forma a presença de comodidades críticas (como ar-condicionado, piscina, Wi-Fi e vista para o mar) valoriza a diária de imóveis que possuem o mesmo número de quartos?
3. **Reputação e Selo de Qualidade:** Anúncios geridos por anfitriões com o selo de *Superhost* ou pontuação de avaliação histórica acima de 4,8 praticam diárias significativamente superiores aos anfitriões comuns?
4. **Perfil Operacional (Profissional vs. Amador):** Qual é a proporção de anfitriões "multimóveis" (profissionais que gerenciam mais de uma propriedade) no mercado carioca e qual a sua participação na oferta total de acomodações?
5. **Tipologia e Capacidade de Acomodação:** Como o preço mediano da diária varia conforme o tipo de acomodação (lugar inteiro, quarto privativo, quarto compartilhado) em relação à capacidade máxima de hóspedes?

---

### Resumo e Estrutura dos Dados Brutos
Os dados brutos foram obtidos do repositório público **Inside Airbnb** e contêm informações detalhadas dos anúncios ativos na cidade do Rio de Janeiro. A estrutura principal consiste nos seguintes atributos originais:

* **Atributos de Identificação e Anfitrião:** `id`, `listing_url`, `host_id`, `host_name`, `host_since`, `host_is_superhost`, `host_listings_count`.
* **Atributos Geográficos:** `neighbourhood_cleansed`, `latitude`, `longitude`.
* **Atributos Estruturais e Tipologia:** `property_type`, `room_type`, `accommodates`, `bedrooms`, `beds`, `bathrooms_text`.
* **Atributos Financeiros e Regras:** `price` (formato texto com símbolo monetário `"$"`), `minimum_nights`, `maximum_nights`.
* **Atributos Qualitativos e Reputacionais:** `amenities` (lista em formato string/array), `number_of_reviews`, `review_scores_rating`.

### Licença de Uso dos Dados
Os dados do Inside Airbnb são disponibilizados publicamente sob a licença **Creative Commons CC0 1.0 Universal (CC0 1.0) / Public Domain Dedication**. A licença permite o uso livre para fins acadêmicos, de pesquisa e desenvolvimento de portfólio tecnológico, desde que mantida a citação da fonte original.

---

## Carga dos Dados (Etapa 4.2)
*(Seção a ser preenchida após a execução do script `01_ingestion_bronze` no Databricks)*
- Explicação da carga na nuvem (Upload para Volume/DBFS no Databricks).
- Link para o script no GitHub.
- Screenshots de evidência da carga.

---

## Modelagem e Catálogo de Dados (Etapa 4.3)
*(Seção a ser preenchida após a execução do script `03_modeling_gold` no Databricks)*
- Descrição da arquitetura do modelo de dados (Star Schema / Tabela Fato e Dimensões).
- Catálogo de dados transcrito (Unity Catalog) contendo descrição, tipo de dado, domínio e linhagem.
- Screenshots do Catálogo e Grafo de Linhagem do Databricks.

---

## Pipeline de Dados (Etapa 4.4)
*(Seção a ser preenchida após a construção dos notebooks no Databricks)*
- Explicação da modularização do pipeline por camadas (Bronze -> Silver -> Gold).
- Detalhamento das transformações aplicadas em cada etapa (o quê, por quê e impacto).
- Links para os scripts versionados.
- Screenshots que evidenciam a persistência das tabelas Delta no ambiente de nuvem.

---

## Qualidade de Dados (Etapa 4.5)
*(Seção a ser preenchida após as análises do script `02_transformation_silver`)*
- Avaliação detalhada das 5 dimensões de qualidade: Completude, Consistência, Unicidade, Acurácia e Outliers.
- Documentação das decisões de tratamento aplicadas durante o ETL.

---

## Análise de Dados (Etapa 4.5)
*(Seção a ser preenchida após as consultas no script `04_analytics_insights`)*
- Respostas técnicas via SQL e PySpark para cada uma das 5 Perguntas de Negócio.
- Discussão e interpretação dos resultados do ponto de vista imobiliário e estratégico.
- Screenshots das consultas executadas e das tabelas/gráficos resultantes.

---

## Autoavaliação
*(Seção a ser preenchida ao término da entrega)*
- Discussão sobre o atingimento dos objetivos traçados.
- Dificuldades encontradas na execução na nuvem (Databricks Free Edition).
- Propostas de trabalhos futuros e integração direta com o pipeline do MVP de Machine Learning.
