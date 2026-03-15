import pandas as pd
from pathlib import Path
import unicodedata

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

p = Path('../bronze/translation.parquet')
df = pd.read_parquet(p)

print(df.columns.tolist())

colunas_texto = ['product_category_name', 'product_category_name_english']
def texto_padronizado(df, colunas):
    """
    Padronizar as colunas de texto de um data frame
    """
    df_copia = df.copy()

    for col in colunas:
        if col in df_copia.columns:
            df_copia[col] = (df_copia[col].astype(str)
            .str.lower()
            .str.strip()
            .apply(
                lambda x: unicodedata.normalize('NFKD', x)
                .encode("ascii", "ignore")
                .decode("utf-8")
            )
            )
        else:
            print(f"Aviso: A coluna'{col}' não foi encontrada.")
    return df_copia

df_padronizado = texto_padronizado(df, colunas_texto)

print(df_padronizado.head())

#Salvar
output = Path('../prata/translation.parquet')
df_padronizado.to_parquet(output, index=False)
print("Salvo com sucesso!")