import pandas as pd
from pathlib import Path

#Configuração de colunas
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

#carregar dados
p = Path('../bronze/order_items.parquet')
df = pd.read_parquet(p)

print(df.columns.tolist())

# ================
# REGRAS DE NEGÓCIO
# ================

#valor total do item
df['valor_total_item'] = df['price'] + df['freight_value']

#proporção do frete no item
df['proporcao_frete'] = df['freight_value'] / df['price']

#item com frete maior que produto
df['frete_maior_produto'] = df['freight_value'] > df['price']

#quantidade itens por pedido
df['qtd_itens_pedido'] = df.groupby('order_id')['order_item_id'].transform('count')

# ===============
# SALVAR
# ===============

output = Path('../prata/order_items.parquet')
df.to_parquet(output, index=False)

print('Salvo com sucesso!')
print(df[['valor_total_item', 'proporcao_frete', 'frete_maior_produto', 'qtd_itens_pedido']].head())