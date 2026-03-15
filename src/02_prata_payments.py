import pandas as pd
from pathlib import Path

#configuração de colunas
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

#carregar dados
p = Path('../bronze/payments.parquet')
df = pd.read_parquet(p)

print(df.columns.tolist())

# ===============
# REGRAS DE NEGÓCIO
# ===============

#Pagamentos parcelados
df['pagamento_parcelado'] = (df['payment_installments'] > 1).astype(bool)

#pagamentos multiplos
df['pagamento_multiplo'] = (df['payment_sequential'] > 1).astype(bool)

# ===============
# Salvar
# ===============

#verificar tipagem
print(df.dtypes)

output = Path('../prata/payments.parquet')
df.to_parquet(output,index=False)

print('Salvo com sucesso!')
print(df[['pagamento_parcelado', 'pagamento_multiplo']].head())