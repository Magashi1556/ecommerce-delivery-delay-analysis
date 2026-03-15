import pandas as pd
from pathlib import Path

#Configurar colunas
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

#Carregar dados parquet
p = Path('../bronze/customers.parquet')
df = pd.read_parquet(p)

#Normalização das colunas
df['customer_city'] = (
    df['customer_city']
    .str.strip()
    .str.lower()
)

df['customer_state'] = (
    df['customer_state']
    .str.strip()
    .str.upper()
)

#indicador de cliente unico
df['cliente_unico'] = df['customer_unique_id']

#validação
print('Tipos:\n')
print(df.dtypes)
print('Nulos:\n')
print(df.isna().sum())

print(df.columns.tolist())
#mapeando coluna região
map_regioes = {
    'Norte': ['AC','AP','AM','PA','RO','RR','TO'],
    'Nordeste': ['AL','BA','CE','MA','PB','PE','PI','RN','SE'],
    'Centro-Oeste': ['DF','GO','MT','MS'],
    'Sudeste': ['ES','MG','RJ','SP'],
    'Sul': ['PR','RS','SC']
}
#aplicando
def mapear_regiao(uf):
    for regiao, estado in map_regioes.items():
        if uf in estado:
            return regiao
    return 'Desconhecido'

df['customer_region'] = df['customer_state'].apply(mapear_regiao)

#======
#Salvar
#======
output = Path('../prata/customers.parquet')
df.to_parquet(output, index=False)

print('Salvo com sucesso!')
print(df.head())