import pandas as pd
from pathlib import Path

#configurar colunas
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

#Carregar dados
p = Path("../raw/olist_geolocation_dataset.csv")
df = pd.read_csv(p, sep=',', encoding='utf-8', low_memory=False)

#Visualização
print("=== HEAD ===")
print(df.head())
print("\n=== TAIL ===")
print(df.tail())
print("\n=== SHAPE ===")
print(f'linhas: {df.shape[0]} colunas: {df.shape[1]}')
print("\n=== INFO ===")
print(df.info())

#Verificar nulos
print("\n=== NULOS ===")
print(df.isna().sum())

#salvar
output = Path("../bronze/geolocation.parquet")
df.to_parquet(output, index=False)
print("\nSalvo com sucesso!")