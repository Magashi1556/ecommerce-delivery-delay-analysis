import pandas as pd
from pathlib import Path

#configurar colunas
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

#carregar dados parquet
p = Path('../bronze/orders.parquet')
df = pd.read_parquet(p)

print(df.columns.tolist())
# ===========================
# REGRAS DE NEGÓCIO
# ===========================

#Indicadores de entrega
df['pedido_entregue'] = df['order_delivered_customer_date'].notna()

#tempo até aprovação
df['tempo_aprovacao'] = (
    df['order_approved_at'] - df['order_purchase_timestamp']
)

#tempo total de entrega
df['tempo_entrega'] = (
    df['order_delivered_customer_date'] - df['order_purchase_timestamp']
)

df.loc[~df['pedido_entregue'], 'tempo_entrega'] = pd.NaT

#tempo de atraso
df['atraso_entrega'] = (
    df['order_delivered_customer_date'] > df['order_estimated_delivery_date']
)

df.loc[~df['pedido_entregue'], 'atraso_entrega'] = False

# atraso na entrega
df['atraso_entrega'] = (
    df['pedido_entregue'] &
    (df['order_delivered_customer_date'] > df['order_estimated_delivery_date'])
)

# dias de atraso
df['dias_atraso'] = None

mask_atraso = df['atraso_entrega']

df.loc[mask_atraso, 'dias_atraso'] = (
    df.loc[mask_atraso, 'order_delivered_customer_date']
    - df.loc[mask_atraso, 'order_estimated_delivery_date']
).dt.days

df['dias_atraso'] = df['dias_atraso'].fillna(0).astype('int64')

df['dias_atraso'].describe()
df['atraso_entrega'].value_counts()

# =============
#SALVAR
# =============

output_path = Path('../prata/orders.parquet')
df.to_parquet(output_path, index=False)

print("Prata orders criado com sucesso")
print(df[['pedido_entregue', 'tempo_entrega', 'atraso_entrega', 'dias_atraso', 'tempo_aprovacao']].head())
