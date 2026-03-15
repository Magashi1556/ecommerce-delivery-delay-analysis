import pandas as pd
from pathlib import Path

#configurando colunas
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

#carregando dados
p = Path('../raw/product_category_name_translation.csv')
df = pd.read_csv(p, sep=',', encoding='utf-8', low_memory=False)

#Visualização
print('=== HEAD ===')
print(df.head())
print('\n=== TAIL ===')
print(df.tail())
print('\n=== SHAPE ===')
print(f'Linhas:{df.shape[0]}, Colunas:{df.shape[1]}')
print('\n=== INFO ===')
print(df.info())

#Verificar nulos
print('\n=== NULOS ===')
print(df.isna().sum())

#verificar tipagem
print('\n=== TIPOS ===')
print(df.dtypes)

#Salvar
output = Path('../bronze/translation.parquet')
df.to_parquet(output, index=False)
print("Salvo com sucesso!")