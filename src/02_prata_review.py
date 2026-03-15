import pandas as pd
from pathlib import Path

#Configuração de colunas
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

#Carregamento dos dados
p = Path("../bronze/reviews.parquet")
df = pd.read_parquet(p)

print(df.head())
print(df.columns.tolist())

#normalizaçao de colunas
df['review_comment_title'] = (
    df['review_comment_title']
    .str.lower()
    .str.strip()
)

df['review_comment_message'] = (
    df['review_comment_message']
    .str.lower()
    .str.strip()
)

#tratar nulos
df['tem_titulo'] = df['review_comment_title'].notna()
df['tem_comentario'] = df['review_comment_message'].notna()

#Verificar tipagem
print(df.dtypes)

# ===========
# Salvar
# ===========

output = Path("../prata/reviews.parquet")
df.to_parquet(output, index=False)

print("Salvo com sucesso!")
print(df[['tem_titulo', 'tem_comentario']].head())