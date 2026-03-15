"""
Projeto: Análise de Atrasos Logísticos no E-commerce Brasileiro

Objetivo:
Investigar como atrasos na entrega impactam a satisfação do cliente.

Dataset:
Brazilian E-Commerce Public Dataset by Olist

Etapas:
1. Construção do dataset analítico (Gold)
2. Feature engineering
3. Análise exploratória
4. Modelagem estatística
5. Extração de insights logísticos
"""

# ==============================================
# SETUP DO AMBIENTE
# configuração dde bibliotecas e parâmetros
# ==============================================
import pandas as pd
import statsmodels.api as sm
from pathlib import Path
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.stats import pearsonr, spearmanr, kendalltau
#Configurando colunas
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

# ==============================================
# INGESTÃO DE DADOS
# Carregamento das tabelas da camada Prata
# ==============================================
BASE_PATH = Path("../prata")
OURO_PATH = Path("../ouro")
GRAFICOS_PATH = OURO_PATH / "graficos"

GRAFICOS_PATH.mkdir(parents=True, exist_ok=True)
# ==============================================
# Carregando da tabela order
# ==============================================
#Carregamento e seleção das colunas da tabela Orders
orders = pd.read_parquet(
    BASE_PATH / "orders.parquet",
    columns=[
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
        "order_delivered_customer_date",
        "order_estimated_delivery_date"
    ]
)

# ==============================================
# Carregando da tabela order_itens
# ==============================================
#Carregamento e seleção das colunas da tabela Orders_items
order_items = pd.read_parquet(
    BASE_PATH / "order_items.parquet",
    columns=[
    "order_id",
    "price",
    "freight_value"

    ]
)
#Criando agregação da tabela order_items
order_items_agg = (
    order_items
    .groupby("order_id")
    .agg(
        valor_total_produtos=("price", "sum"),
        valor_total_frete=("freight_value", "sum"),
        qtd_itens=("price", "count")
    )
    .reset_index()
)
# ==============================================
# Carregando da tabela payments
# ==============================================
#Carregamento e seleção das colunas da tabela payments
payments = pd.read_parquet(
    BASE_PATH / "payments.parquet",
    columns=[
        "order_id",
        "payment_type",
        "payment_installments",
        "payment_value"
    ]
)
#criando agregação da tabela payment
payments_agg = (
    payments
    .groupby("order_id")
    .agg(
        valor_total_pago=("payment_value", "sum"),
        qtd_parcelas=("payment_installments", "max"),
        tipo_principal_pagamento=("payment_type",
                                   lambda x: x.mode()[0] if not x.mode().empty else None)
    )
    .reset_index()
)

# ==============================================
# Carregando da tabela review
# ==============================================
#Carregamento e seleção das colunas da tabela review
reviews = pd.read_parquet(
    BASE_PATH / "reviews.parquet",
    columns=[
        "order_id",
        "review_score"
    ]
)
#criando agregação da tabela reviw
reviews_agg = (
    reviews
    .groupby("order_id")
    .agg(
        nota_media_review=("review_score", "mean")
    )
    .reset_index()
)
# ==============================================
# Carregando da tabela customers
# ==============================================
#Carregamento e seleção das colunas da tabela customers
customers = pd.read_parquet(
    BASE_PATH / "customers.parquet",
    columns=[
        "customer_id",
        "customer_state"
    ]
)
# ==============================================
# Observação
# ==============================================
"""
Irei usar a tabela products, sellers e geolocation futuramente
e em customers usarei a coluna cidade tambem futuramente
"""

# ==============================================
# CONSTRUÇÃO DO DATASET OURO
# integração das tabelas em nível de pedido
# ==============================================
gold_orders = (
    orders
    .merge(order_items_agg, on="order_id", how="left")
    .merge(payments_agg, on="order_id", how="left")
    .merge(reviews_agg, on="order_id", how="left")
    .merge(customers, on="customer_id", how="left")
)

# ==============================================
# FEATURE ENGINEERING
#criação de métricas logísticas e financeiras
# ==============================================
#Calculando atraso
gold_orders = gold_orders[gold_orders["order_status"] == "delivered"]
#garantir as datas
gold_orders["order_purchase_timestamp"] = pd.to_datetime(gold_orders["order_purchase_timestamp"], errors= "coerce")
gold_orders["order_delivered_customer_date"] = pd.to_datetime(gold_orders["order_delivered_customer_date"], errors= "coerce")
gold_orders["order_estimated_delivery_date"] = pd.to_datetime(gold_orders["order_estimated_delivery_date"], errors= "coerce")

# ==============================================
# MÉTRICAS LOGÍSTICAS
# ==============================================
#Tempo de entrega
gold_orders["tempo_entrega_dias"] = (
    gold_orders["order_delivered_customer_date"]
    - gold_orders["order_purchase_timestamp"]
).dt.days

# Atraso
gold_orders["dias_atraso"] = (
    gold_orders["order_delivered_customer_date"]
    - gold_orders["order_estimated_delivery_date"]
).dt.days

#Flag de atraso
gold_orders["pedido_atrasado"] = gold_orders["dias_atraso"] > 0

# ==============================================
# MÉTRICAS FINANCEIRAS
# ==============================================
#Valor total  final do pedido
gold_orders["valor_total_pedido"] = (
    gold_orders["valor_total_produtos"]
    + gold_orders["valor_total_frete"]
)
#ticket médio por pedido
gold_orders["ticket_medio_pedido"] = gold_orders["valor_total_pedido"] / gold_orders["qtd_itens"]

# ==============================================
# MÉTRICAS TEMPORAIS
# ==============================================
gold_orders["ano_compra"] = gold_orders["order_purchase_timestamp"].dt.year
gold_orders["mes_compra"] = gold_orders["order_purchase_timestamp"].dt.month
gold_orders["trimestre"] = gold_orders["order_purchase_timestamp"].dt.quarter
#mapeando dias
mapa = {
    "Monday": "segunda", "Tuesday": "terca", "Wednesday": "quarta",
    "Thursday": "quinta", "Friday": "sexta", "Saturday": "sabado", "Sunday": "domingo"
}
gold_orders["dia_semana"] = gold_orders["order_purchase_timestamp"].dt.day_name().map(mapa)

# ==============================================
# ENGENHARIA DE ATRASO
# ==============================================
#analises executivas
gold_orders["dias_adiantado"] = gold_orders["dias_atraso"] < 0
gold_orders["dias_atraso_real"] = gold_orders["dias_atraso"] > 0
'''
atraso_positivo = coluna sem os dias em negativo que seria a entrega adiantada
adiantado_dias = coluna somente com os dias adiantados (contabilizando os dias que estavam em negativo)
'''
gold_orders["adiantado_dias"] = (-gold_orders["dias_atraso"]).clip(lower=0)
gold_orders["atraso_positivo"] = gold_orders["dias_atraso"].clip(lower=0)

# ======================================
# ANÁLISE EXPLORATÓRIA
# investigando impacto do atraso na avaliação do cliente
# ======================================
#Verificação de possiveis nulos e tipos
print(gold_orders[["atraso_positivo", "nota_media_review"]].isna().sum())
print(gold_orders["nota_media_review"].dtype)
print(gold_orders["atraso_positivo"].dtype)
print(gold_orders["atraso_positivo"].nunique())
print(gold_orders["nota_media_review"].nunique())

# ======================================
# VERIDICAÇÃO DE QUALIDADE
# ======================================
#validando o left join
print(gold_orders.isna().mean().sort_values(ascending=False))

# ======================================
# SEGMENTAÇÃO DO ATRASO
# ======================================
#criando categorias para atrasos
gold_orders["categoria_atraso"] = pd.cut(
    gold_orders["atraso_positivo"],
    bins=[-1, 0, 3, 7, 14, 999],
    labels=[
    "Sem atraso",
    "Leve (1-3)",
    "Moderado (4-7)",
    "Alto (8-14)",
    "Critico (>14)"
    ]
)
#fazendo comparação
analise_atraso = (
    gold_orders
    .groupby("categoria_atraso", observed=True)["nota_media_review"]
    .agg(["count", "mean", "median"])
    .sort_index()
)

analise_atraso

# ======================================
# VISUALIZAÇÃO EXPLORATÓRIA
# ======================================
#gerando grafico
plt.figure(figsize=(10, 5))
sns.boxenplot(data=gold_orders,
              x="categoria_atraso",
              y="nota_media_review")

plt.title("Atrasos maiores estão associados a avaliações significativamente mais baixas")
plt.xticks(rotation=30)
plt.xlabel("Dias de atraso")
plt.ylabel("Nota média do cliente")
plt.savefig(GRAFICOS_PATH/"impacto_atraso_review.png", dpi=300, bbox_inches="tight")
plt.show()

# ======================================
# ANÁLISE ESTATÍSTICA
# testando relação entre atraso e avaliação
# ======================================
'''
Após visualizar o grafico decidi usar Spearman, devido a natureza ordinal da variavel nota_media_review
e relação monotonica esperada entre atraso e avaliação
'''

df_corr = gold_orders[
    (gold_orders["atraso_positivo"].notna()) &
    (gold_orders["nota_media_review"].notna())
]

# ======================================
# CORRELAÇÃO DE SPEARMAN
# ======================================
corr, p = spearmanr(
    df_corr["atraso_positivo"],
    df_corr["nota_media_review"]
)
print("Spearman:", corr)
print("p-value:", p)
print("Tamanho da amostra:", df_corr.shape[0])

# ======================================
# Regressão Linear Simples
# ======================================
x = df_corr["atraso_positivo"]
y = df_corr["nota_media_review"]

x = sm.add_constant(x)

modelo_linear = sm.OLS(y, x).fit()

print(modelo_linear.summary())
"""
nota = 4.2079 -0.0741 * atraso_positivo
Nota media se aproxima de 4.2
Cada 1 dia adicional de atraso reduz a nota media em 0.074
R² = 0.069 (6.9% da nota é explicada pelo atraso)
Interpretação:
    Atraso impacta significativamente, mas não é o principal determinante da avaliação.

Logisticamente importa.
Mas  não explica sozinha a satisfação.
"""
#testando e comparando *importante*
print(df_corr[df_corr["atraso_positivo"] == 0]["nota_media_review"].mean())
print(df_corr["nota_media_review"].mean())
# ======================================
# REGRESSÃO QUADRÁTICA
# ======================================
#teste de vies de seleção
print(gold_orders.groupby("pedido_atrasado")["nota_media_review"].count())

#dataset modelo
df_model= gold_orders[
    ["atraso_positivo", "nota_media_review"]
].dropna()

#criando variavel quadratica
df_model["atraso_quadrado"] = df_model["atraso_positivo"] ** 2

#separando x e y
x = df_model[["atraso_positivo", "atraso_quadrado"]] #variaveis explicativas
x = sm.add_constant(x) #adcionando intercepto
y = df_model["nota_media_review"] #variavel alvo

#Rodando a regressão
modelo_quadratico = sm.OLS(y, x).fit()

#Visualizando atraso_positivo vs nota_media_review
plt.figure(figsize=(10, 5))
sns.regplot(
    data=df_model,
    x="atraso_positivo",
    y="nota_media_review",
    order=2
)
plt.title("Relação não linear entre atraso na entrega e avaliação do cliente")
plt.xticks(rotation=30)
plt.xlabel("Dias de atraso")
plt.ylabel("Nota média do cliente")
plt.savefig(GRAFICOS_PATH/"relacao_nao_linear.png", dpi=300, bbox_inches="tight")
plt.show()

print(modelo_quadratico.summary())

# ======================================
# ANÁLISE DE IMPACTO DO ATRASO
# identificando zonas críticas de atraso logístico
# ======================================
#Testando Bins de Atraso
gold_orders["faixa_atraso"] = pd.cut(
    gold_orders["atraso_positivo"],
    bins=[0,2,5,10,20,50,200],
    labels=["0-2", "2-5", "5-10", "10-20", "20-50", "50+"]
)

print(gold_orders.groupby("faixa_atraso", observed=True)["nota_media_review"].mean())
print(gold_orders["faixa_atraso"].value_counts())

#visualizando
plt.figure(figsize=(10, 5))
sns.barplot(
    data=gold_orders,
    x="faixa_atraso",
    y="nota_media_review"
)
plt.title("Avaliações caem drasticamente após 5 dias de atraso")
plt.xticks(rotation=30)
plt.xlabel("Dias de atraso")
plt.ylabel("Nota média do cliente")
plt.savefig(GRAFICOS_PATH/"avaliacoes_caindo.png", dpi=300, bbox_inches="tight")
plt.show()

#salvando
output = Path('../ouro/gold_orders.parquet')
gold_orders.to_parquet(output, index=False)

#Gerando tabela de insights
impacto_atraso = (
    gold_orders
    .groupby("faixa_atraso", observed=True)
    .agg(
        pedidos=("order_id", "count"),
        nota_media=("nota_media_review", "mean")
    )
)

impacto_atraso.to_csv(
    OURO_PATH /"impacto_atraso.csv",
    index=True
)