import pandas as pd
from pathlib import Path

#configurar colunas
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

#Carregar dados
p = Path("../bronze/seller.parquet")
df = pd.read_parquet(p)

print("\n === COLUNAS ===\n")
print(df.columns.tolist())

# ===============
# REGRAS DE NEGOCIOS
# ===============
df['seller_city'] = (
    df['seller_city']
    .str.lower()
    .str.strip()
)
print(df['seller_city'].head())

#criando boolean city
df['seller_tem_cidade'] = df['seller_city'].notnull()
print(df['seller_tem_cidade'].head())

# ===============
# SALVAR
# ===============
output = Path('../prata/sellers.parquet')
df.to_parquet(output, index=False)
print("Salvo com sucesso!")