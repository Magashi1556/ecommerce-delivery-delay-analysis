import pandas as pd
from pathlib import Path

#configuração de colunas
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

#Leitura do dataframe
p = Path('../raw/olist_order_items_dataset.csv')
dados = pd.read_csv(p, sep=',', encoding='utf-8', low_memory=False)

#Visualização
print('=== HEAD ===')
print(dados.head())
print('\n=== TAIL ===')
print(dados.tail())
print('\n=== INFO ===')
print(dados.info())
print('\n=== SHAPE ===')
print(f'Linhas:{dados.shape[0]} Colunas:{dados.shape[1]}')

#Verificar valores nulos
print('\n=== NULLS ===')
print(dados.isna().sum())

#Validação de colunas
print('\n=== COLUNAS ===')
print(dados.columns.tolist())

#convertendo coluna data
cols_datetime = ['shipping_limit_date']

#validando colunas antes de usar
missing_cols = set(cols_datetime) - set(dados.columns)
if missing_cols:
    raise ValueError(f'Colunas ausentes: {missing_cols}')

for col in cols_datetime:
    dados[col] = pd.to_datetime(dados[col], errors='coerce')

print("\n=== IDs ===")
print("order_id e order_item duplos:", dados.duplicated(subset=['order_id', 'order_item_id']).sum())

#leitura de tipagem novamente
print('\n====  TIPOS ====')
print(dados.dtypes)

output = Path('../bronze/order_items.parquet')
dados.to_parquet(output, index=False)