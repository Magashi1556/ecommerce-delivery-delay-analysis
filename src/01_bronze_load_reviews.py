import pandas as pd
from pathlib import Path

#configuração de colunas
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

#carregamento dos dados
p = Path('../raw/olist_order_reviews_dataset.csv')
dados = pd.read_csv(p, sep=',', encoding='utf-8', low_memory=False)

#leitura dos dados
print('=== HEAD ===')
print(dados.head())
print('\n=== TAIL ===')
print(dados.tail())
print('\n=== INFO ===')
print(dados.info())
print('\n=== SHAPE ===')
print(f'Linhas: {dados.shape[0]}, Colunas: {dados.shape[1]}')

#verificar nulos
print('\n=== NULOS ===')
print(dados.isna().sum())

#validar coluna
print('\n=== COLUNAS ===')
print(dados.columns.tolist())

#converter colunas de datas
cols_datetime = [
    'review_creation_date',
    'review_answer_timestamp'
]

#validando colunas antes do uso
cols_ausente = set(cols_datetime) - set(dados.columns)
if cols_ausente:
    raise ValueError(f'Colunas ausentes: {cols_ausente}')

for col in cols_datetime:
    dados[col] = pd.to_datetime(dados[col], errors='coerce')

#validar ids
print('\n=== IDS ===')
print("nulos:", dados['review_id'].isna().sum())
print("duplos:", dados['review_id'].duplicated().sum())


#verificar tipos
print('\n=== TIPOS ===')
print(dados.dtypes)

#salvar em parquet
output = Path('../bronze/reviews.parquet')
dados.to_parquet(output, index=False)