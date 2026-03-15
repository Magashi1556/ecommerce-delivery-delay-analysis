import pandas as pd
from pathlib import Path

#configurando colunas
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

#Carregando tabela
p = Path('../raw/olist_customers_dataset.csv')
df = pd.read_csv(p, sep=',', encoding='utf-8', low_memory=False)

#visualizações
print('=== HEAD ===')
print(df.head())
print('\n=== TAIL ===')
print(df.tail())
print('\n=== SHAPE ===')
print(f'Linhas: {df.shape[0]}, Colunas: {df.shape[1]}')
print('\n=== INFO ===')
print(df.info())

#verificando nulos
print('\n=== NULOS ===')
print(df.isna().sum())

#Validação de colunas
print('\n=== COLUNAS ===')
print(df.columns.tolist())

#Validar IDs
print("\n=== IDs ===")
print(df['customer_id'].isna().sum())
print(df['customer_id'].duplicated().sum())

#Salvar
output = Path('../bronze/customers.parquet')
df.to_parquet(output, index=False)