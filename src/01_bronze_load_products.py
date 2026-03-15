import pandas as pd
from pathlib import Path


#Configurando colunas
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

#Carregando dados
p = Path("../raw/olist_products_dataset.csv")
df = pd.read_csv(p, sep=',', encoding='utf-8', low_memory=False)

#vizualizações
print("=== HEAD ===")
print(df.head())
print("\n=== TAIL ===")
print(df.tail())
print("\n=== SHAPE ===")
print(df.shape)
print("\n=== INFO ===")
print(df.info())

#verificar nulos
print("\n=== NULOS ===")
print(df.isna().sum())

#validação de colunas
print("\n=== COLUNAS ===")
print(df.columns.tolist())

colunas_esperadas = {
    'product_id',
    'product_category_name',
    'product_name_lenght',
    'product_description_lenght',
    'product_photos_qty',
    'product_weight_g',
    'product_length_cm',
    'product_height_cm',
    'product_width_cm'
}

ausente = colunas_esperadas - set(df.columns)
extra = set(df.columns) - colunas_esperadas

print("Colunas Faltantes:", ausente)
print("Colunas Extra:", extra)

#Verificar valores negativos
print("\n=== VALORES NEGATIVOS ==")
print((df[[
'product_weight_g',
     'product_length_cm',
     'product_height_cm',
     'product_width_cm']] < 0).sum())

#Validando IDs
print("\n=== IDs ===")
print(df['product_id'].isna().sum())
print(df['product_id'].duplicated().sum())

#Describe
print("\n=== DESCRIBE ===")
print(df['product_photos_qty'].describe())
print(df['product_weight_g'].describe())


#Salvando
output = Path('../bronze/products.parquet')
df.to_parquet(output, index=False)
print("\nSalvo com sucesso!")