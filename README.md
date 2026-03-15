# Contexto do problema

Este projeto tem como objetivo investigar se atrasos logísticos em entregas de e-commerce influenciam a satisfação do cliente, medida pela nota de avaliação do pedido (review score).

A análise busca responder à seguinte pergunta:
  Atrasos na entrega impactam significativamente a avaliação dos clientes?
---
# Dataset

Dataset utilizado:
Olist Brazilian E-Commerce Public Dataset
https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

Os dados foram obtidos a partir do Kaggle e armazenados localmente para garantir reprodutibilidade e controle das etapas do pipeline de dados.

O dataset contém informações sobre:
∙ pedidos
∙ clientes
∙ produtos
∙ vendedores
∙ pagamentos
∙ avaliações
∙ logística de entrega
---
# Pipeline de dados

O projeto foi estruturado seguindo o conceito de Medallion Architecture, com separação das camadas de dados.
## Raw
Contém os dados originais em formato CSV exatamente como disponibilizados no Kaggle.

## Bronze

Etapa de ingestão dos dados, onde os arquivos CSV foram carregados e convertidos para formato Parquet, permitindo maior eficiência de leitura e armazenamento.

## Prata (Silver)

Nesta camada foram realizadas:
∙ limpeza de dados
∙ padronização de colunas
∙ tratamento de inconsistências
∙ preparação das tabelas para análise

## Ouro (Gold)

Camada analítica final onde foi construído o dataset consolidado de pedidos (gold_orders), incluindo:
∙ métricas logísticas
∙ métricas financeiras
∙ variáveis temporais
∙ variáveis derivadas de atraso
Essa camada foi utilizada para realizar a análise exploratória e estatística.
---
# Metodologia

A metodologia adotada foi focada na análise da relação entre:
∙ dias de atraso na entrega
∙ nota de avaliação do cliente

As seguintes técnicas foram utilizadas:
∙ Análise exploratória de dados (EDA)
∙ Visualização estatística
∙ Correlação de Spearman
∙ Regressão linear
∙ Regressão quadrática para investigar possíveis relações não lineares
A correlação de Spearman foi escolhida devido à natureza ordinal da variável de avaliação (review score).
---
# Análise estatística

A análise estatística mostrou uma correlação negativa moderada entre atraso na entrega e avaliação do cliente.

Resultados principais:
∙ Correlação de Spearman: -0.31
∙ Regressão linear com R² ≈ 0.069

Interpretação:
O atraso explica aproximadamente 6,9% da variação nas avaliações dos clientes.

Isso indica que:
∙ o atraso tem impacto estatisticamente significativo
∙ porém não é o único fator que influencia a satisfação do cliente
Outros fatores provavelmente influenciam a avaliação, como:
∙ qualidade do produto
∙ experiência de compra
∙ comunicação com o vendedor
∙ condições de entrega
---
# Resultados

A análise por faixas de atraso revelou um ponto crítico na experiência do cliente.
Pedidos com mais de 5 dias de atraso apresentam queda significativa na avaliação média.
Esse comportamento indica uma zona crítica de atraso logístico, onde a percepção negativa do cliente aumenta de forma acentuada.
---
# Insights

Principais insights obtidos:
∙ A nota média geral do dataset é aproximadamente 4.2
∙ Cada dia adicional de atraso reduz a avaliação média em cerca de 0.074 pontos
∙ Atrasos superiores a 5 dias apresentam forte impacto negativo na satisfação do cliente
Esses resultados reforçam a importância da eficiência logística no e-commerce, especialmente no controle de atrasos.
---
# Limitações do estudo

Este projeto focou exclusivamente na relação entre atraso logístico e avaliação do cliente.
Outras variáveis relevantes não foram exploradas neste estudo, como:
∙ tipo de produto
∙ localização geográfica
∙ desempenho de vendedores
∙ valor do frete
O dataset possui potencial para análises adicionais que podem ampliar os insights obtidos.
