import pandas as pd
from pathlib import Path

#configurando colunas
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

#leitura do dataframe
p = Path('../raw/olist_orders_dataset.csv')
dados = pd.read_csv(p, sep=',', encoding='utf-8', low_memory=False)

#visualização da tabela
print('==== HEAD ====')
print(dados.head())
print('\n==== TAIL ====')
print(dados.tail())
print('\n==== INFO ====')
print(dados.info())
print('\n==== SHAPE ====')
print(f'Linhas:{dados.shape[0]} Colunas:{dados.shape[1]}')

#Verificação de valores nulos
print('\n=== NULLS ===')
print(dados.isna().sum())

#validação de colunas
print('\n==== COLUNAS ====')
print(dados.columns.tolist())

#converter data
cols_datetime = [
    'order_purchase_timestamp',
    'order_approved_at',
    'order_delivered_carrier_date',
    'order_delivered_customer_date',
    'order_estimated_delivery_date'
]
#validando colunas antes de usar
missing_cols = set(cols_datetime) - set(dados.columns)
if missing_cols:
    raise ValueError(f'Colunas ausentes: {missing_cols}')

for col in cols_datetime:
    dados[col] = pd.to_datetime(dados[col], errors='coerce')


#leitura de tipagem novamente
print('\n====  TIPOS ====')
print(dados.dtypes)

print("\n=== IDs ===")
print("\norder_id nulos:", dados['order_id'].isna().sum())
print("\norder_id duplus:", dados['order_id'].duplicated().sum())
print("\ncustomer_id nulos:", dados["customer_id"].isna().sum())

output = Path('../bronze/orders.parquet')
dados.to_parquet(output, index=False)



