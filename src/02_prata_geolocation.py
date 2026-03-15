import pandas as pd
from pathlib import Path

#Configuração de colunas
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

#Carregar dados
p = Path('../bronze/geolocation.parquet')
df = pd.read_parquet(p)

#Visualização
print(df.head())

#Validação de colunas
colunas_utilizadas = [
    'geolocation_zip_code_prefix',
    'geolocation_city',
    'geolocation_state'
]

colunas_esperadas = set(colunas_utilizadas)
colunas_df = set(df.columns)

colunas_faltantes = colunas_esperadas - colunas_df

if colunas_faltantes:
    raise ValueError(f'Colunas ausentes: {Colunas_faltantes}')

df = df[colunas_utilizadas]
print("\nColunas utilizadas:")
print(df.head())

#Padronização e normalização
df = (
    df[colunas_utilizadas]
    .assign(
        geolocation_city=lambda x: x['geolocation_city'].str.lower().str.strip()
    )
    .drop_duplicates(subset='geolocation_zip_code_prefix')
)

#validação de tipagem
print("\n === VERIFICAR TIPOS ===")
print(df.dtypes)

# ===============
# SALVAR
# ===============
output = Path('../prata/geolocation.parquet')
df.to_parquet(output, index=False)
print("\n Salvo com sucesso!")