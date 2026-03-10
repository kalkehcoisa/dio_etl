import pandas as pd

df = pd.read_csv("bank_churn_mensagens.csv")
print(df[["nome", "country", "balance", "perfil", "mensagem_ia"]].head(10).to_string())
