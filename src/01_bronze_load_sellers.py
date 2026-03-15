import pandas as pd
from pathlib import Path

#configurando colunas
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

#carregando dados
p = Path('../raw/olist_sellers_dataset.csv')
df = pd.read_csv(p, sep=',', encoding='utf-8', low_memory=False)

#Visualização
print(" === HEAD ===")
print(df.head())
print("\n === TAIL ===")
print(df.tail())
print("\n === SHAPE ===")
print(f'Linhas: {df.shape[0]}, Colunas: {df.shape[1]}')
print('\n === INFO ===')
print(df.info())

#Verificando valores nulos
print('\n === NULLS ===')
print(df.isnull().sum())

#validar ids
print('\n === IDs ===')
print("seller_id: ", df.duplicated(subset=['seller_id']).sum())

#leitura de tipagem
print('\n === TIPOS ===')
print(df.dtypes)

#Salvando
output = Path("../bronze/seller.parquet")
df.to_parquet(output, index=False)
print('\n Salvo com sucesso!')