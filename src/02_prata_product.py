import pandas as pd
from pathlib import Path
import numpy as np
#Configurar colunas
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

#carregar dados parquet
p = Path('../bronze/products.parquet')
df = pd.read_parquet(p)

print("\n === COLUNAS ===\n")
print(df.columns.tolist())

# ===============
# REGRAS DE NEGOCIOS
# ===============
df[['product_category_name']].copy()
df['product_category_name'] = (
    df['product_category_name']
    .str.lower()
    .str.strip()
)
print(df['product_category_name'].head())

print("\n === REGRAS ===\n")
#Produto com categoria
df['produto_tem_categoria'] = df['product_category_name'].notna()
print(df['produto_tem_categoria'].head())

#Produto com descrição
df['produto_tem_descricao'] = df['product_description_lenght'].notna()
print(df['produto_tem_descricao'].head())

#produto tem foto
df['produto_tem_foto'] = df['product_photos_qty'].notna()
print(df['produto_tem_foto'].head())

#Peso Valido
df['peso_valido'] =  df['product_weight_g'] > 0
print(df['peso_valido'].head())

#volume do produto
df['volume_produto'] = df['product_length_cm'] * df['product_height_cm'] * df['product_width_cm']
print(df['volume_produto'].head())

#tamanho da discrição
condicoes = [
    df['product_description_lenght'] < 250,
    df['product_description_lenght'].between(250, 600),
    df['product_description_lenght'] > 600
]

escolhas = ['curta', 'media', 'longa']

df['tamanho_descricao'] = np.select(condicoes, escolhas, default='indefinida')
print(df['tamanho_descricao'].head())

# ===============
# SALVAR
# ===============
output = Path('../prata/products.parquet')
df.to_parquet(output, index=False)
print("\nSalvo com sucesso!\n")