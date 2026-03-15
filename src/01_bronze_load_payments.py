import pandas as pd
from pathlib import Path

#Configuração de colunas
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

#carregamento dos dados
p = Path('../raw/olist_order_payments_dataset.csv')
dados = pd.read_csv(p, sep=',', encoding='utf-8', low_memory=False)

#Visualizar od dados
print('=== HEAD ===')
print(dados.head())
print('\n=== TAIL ===')
print(dados.tail())
print('\n=== INFO ===')
print(dados.info())
print('\n=== SHAPE ===')
print(f'Linhas: {dados.shape[0]}, Colunas: {dados.shape[1]}')

#verificar nulos
print('\n=== NULLS ===')
print(dados.isna().sum())

#Validar colunas
print('\n=== COLUNAS ===')
print(dados.columns.tolist())

expected_cols = [
    'order_id',
    'payment_sequential',
    'payment_type',
    'payment_installments',
    'payment_value'
]

missing_cols = set(expected_cols) - set(dados.columns)
if missing_cols:
    raise ValueError(f'Colunas ausentes: {missing_cols}')

#valida ids
print('\n=== DUPLICATES ===')
print("order_id e payment_sequential duplos:", dados.duplicated(subset=['order_id', 'payment_sequential']).sum())

#leitura da tipagem
print('\n=== DATA TYPES ===')
print(dados.dtypes)

#salvar em parquet
output = Path('../bronze/payments.parquet')
dados.to_parquet(output, index=False)