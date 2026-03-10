"""
Enriquece o Bank Customer Churn (Kaggle) com campos extras no estilo DIO
e injeta ruídos artificiais para prática de ETL.

Campos adicionados:
  nome, email, numero_conta, data_nascimento, cidade, limite_credito
"""

import pandas as pd
import numpy as np
import random
import string

random.seed(42)
np.random.seed(42)

# ── dados para geração dos campos novos ───────────────────────────────────────

NOMES_MASC = [
    "Carlos Eduardo Silva", "João Pedro Souza", "Lucas Ferreira Lima",
    "Rafael Oliveira Costa", "Thiago Alves Pereira", "Bruno Souza Martins",
    "Felipe Lima Santos", "Marcos Pereira Rocha", "Diego Martins Nunes",
    "André Costa Araújo", "Rodrigo Santos Gomes", "Gabriel Rocha Carvalho",
    "Mateus Carvalho Alves", "Leandro Nunes Ferreira", "Vinicius Araújo Lima",
    "Paulo Roberto Mendes", "Fernando Henrique Costa", "Ricardo Borges Silva",
    "Alexandre Moreira Dias", "Gustavo Pinto Ribeiro",
]
NOMES_FEM = [
    "Ana Clara Souza", "Fernanda Lima Costa", "Mariana Costa Santos",
    "Patrícia Rocha Alves", "Beatriz Nunes Ferreira", "Camila Martins Lima",
    "Juliana Alves Pereira", "Letícia Ferreira Silva", "Amanda Oliveira Rocha",
    "Natália Santos Martins", "Larissa Pereira Gomes", "Priscila Rocha Nunes",
    "Vanessa Gomes Carvalho", "Aline Carvalho Araújo", "Renata Souza Ribeiro",
    "Mônica Dias Borges", "Cristiane Moreira Pinto", "Débora Ribeiro Lima",
    "Elaine Pinto Mendes", "Silvana Costa Ferreira",
]
DOMINIOS = ["gmail.com", "hotmail.com", "yahoo.com.br", "outlook.com", "uol.com.br", "terra.com.br"]

CIDADES_FRANCE  = ["Paris", "Lyon", "Marseille", "Bordeaux", "Nice", "Toulouse", "Nantes"]
CIDADES_SPAIN   = ["Madrid", "Barcelona", "Valencia", "Sevilla", "Zaragoza", "Bilbao"]
CIDADES_GERMANY = ["Berlin", "Munich", "Hamburg", "Frankfurt", "Cologne", "Stuttgart"]

COUNTRY_CITIES = {
    "France":  CIDADES_FRANCE,
    "Spain":   CIDADES_SPAIN,
    "Germany": CIDADES_GERMANY,
}

def gerar_nome(genero):
    if genero == "Female":
        return random.choice(NOMES_FEM)
    return random.choice(NOMES_MASC)

def gerar_email(nome):
    partes = nome.lower().split()
    base = f"{partes[0]}.{partes[1]}" if len(partes) >= 2 else partes[0]
    for c, r in [("ã","a"),("á","a"),("â","a"),("é","e"),("ê","e"),
                 ("í","i"),("ó","o"),("ô","o"),("ú","u"),("ç","c")]:
        base = base.replace(c, r)
    return f"{base}@{random.choice(DOMINIOS)}"

def gerar_numero_conta():
    return "".join(random.choices(string.digits, k=5)) + "-" + random.choice(string.digits)

def gerar_data_nascimento(age):
    ano = 2024 - age
    mes = random.randint(1, 12)
    dia = random.randint(1, 28)
    # ~40 % saem em YYYY-MM-DD (ruído de formato)
    if random.random() < 0.40:
        return f"{ano}-{mes:02d}-{dia:02d}"
    return f"{dia:02d}/{mes:02d}/{ano}"

def gerar_cidade(country):
    opcoes = COUNTRY_CITIES.get(country, CIDADES_FRANCE)
    return random.choice(opcoes)

# ── carrega CSV original ──────────────────────────────────────────────────────

df = pd.read_csv("/mnt/user-data/uploads/Bank_Customer_Churn_Prediction.csv")
print(f"Linhas originais : {len(df)}")
print(f"Colunas originais: {list(df.columns)}\n")

# ── adiciona campos novos ─────────────────────────────────────────────────────

df["nome"]            = df["gender"].apply(gerar_nome)
df["email"]           = df["nome"].apply(gerar_email)
df["numero_conta"]    = [gerar_numero_conta() for _ in range(len(df))]
df["data_nascimento"] = df["age"].apply(gerar_data_nascimento)
df["cidade"]          = df["country"].apply(gerar_cidade)
df["limite_credito"]  = (df["balance"] * np.random.uniform(0.5, 3.0, len(df))).round(2)

# reordena colunas — campos de identidade primeiro
col_order = [
    "customer_id", "nome", "email", "numero_conta", "data_nascimento",
    "cidade", "country", "gender", "age", "credit_score", "tenure",
    "balance", "limite_credito", "products_number", "credit_card",
    "active_member", "estimated_salary", "churn"
]
df = df[col_order]

# ── injeta ruídos artificiais ─────────────────────────────────────────────────

def aplicar_ruidos(df):
    df = df.copy()
    n  = len(df)

    # 1. Nomes em MAIÚSCULO (~8 %)
    idx = df.sample(frac=0.08).index
    df.loc[idx, "nome"] = df.loc[idx, "nome"].str.upper()

    # 2. Emails sem @ (~5 %)
    idx = df.sample(frac=0.05).index
    df.loc[idx, "email"] = df.loc[idx, "email"].str.replace("@.*", "", regex=True)

    # 3. Emails sem domínio válido (~4 %)
    idx = df.sample(frac=0.04).index
    df.loc[idx, "email"] = df.loc[idx, "email"].str.replace(r"\.[a-z]+$", "", regex=True)

    # 4. Emails nulos (~4 %)
    idx = df.sample(frac=0.04).index
    df.loc[idx, "email"] = np.nan

    # 5. Saldos negativos (~5 %)
    idx = df.sample(frac=0.05).index
    df.loc[idx, "balance"] = df.loc[idx, "balance"] * -1

    # 6. Outliers absurdos de saldo (~2 %)
    idx = df.sample(frac=0.02).index
    df.loc[idx, "balance"] = 999999.99

    # 7. Salary nulo (~6 %)
    idx = df.sample(frac=0.06).index
    df.loc[idx, "estimated_salary"] = np.nan

    # 8. Idade inválida -1 (~3 %)
    idx = df.sample(frac=0.03).index
    df.loc[idx, "age"] = -1

    # 9. Credit score impossível 999 (~3 %)
    idx = df.sample(frac=0.03).index
    df.loc[idx, "credit_score"] = 999

    # 10. Cidades em minúsculo (~8 %)
    idx = df.sample(frac=0.08).index
    df.loc[idx, "cidade"] = df.loc[idx, "cidade"].str.lower()

    # 11. Linhas duplicadas (~1.5 %)
    dups = df.sample(frac=0.015)
    df = pd.concat([df, dups], ignore_index=True)

    # 12. customer_id nulo (~2 %)
    idx = df.sample(frac=0.02).index
    df.loc[idx, "customer_id"] = np.nan

    return df.sample(frac=1).reset_index(drop=True)  # embaralha

df_dirty = aplicar_ruidos(df)

# ── salva ─────────────────────────────────────────────────────────────────────

output = "/home/claude/bank_churn_dirty.csv"
df_dirty.to_csv(output, index=False, encoding="utf-8-sig")

print(f"Linhas finais    : {len(df_dirty)}  (originais + duplicatas)")
print(f"Colunas finais   : {list(df_dirty.columns)}")
print()
print("=== RESUMO DOS RUÍDOS INJETADOS ===")
print(f"  Nomes em MAIÚSCULO        : {df_dirty['nome'].dropna().apply(lambda x: x.isupper()).sum()}")
print(f"  Emails nulos              : {df_dirty['email'].isna().sum()}")
print(f"  Emails sem @              : {df_dirty['email'].dropna().apply(lambda x: '@' not in str(x)).sum()}")
print(f"  Saldos negativos          : {(df_dirty['balance'] < 0).sum()}")
print(f"  Saldos outlier (999999)   : {(df_dirty['balance'] == 999999.99).sum()}")
print(f"  Idades inválidas (-1)     : {(df_dirty['age'] == -1).sum()}")
print(f"  Credit score > 850        : {(df_dirty['credit_score'] > 850).sum()}")
print(f"  Salary nulos              : {df_dirty['estimated_salary'].isna().sum()}")
print(f"  Customer_id nulos         : {df_dirty['customer_id'].isna().sum()}")
print(f"  Datas formato YYYY-MM-DD  : {df_dirty['data_nascimento'].dropna().apply(lambda x: '-' in str(x)).sum()}")
print(f"  Cidades em minúsculo      : {df_dirty['cidade'].dropna().apply(lambda x: x == x.lower() and x != '').sum()}")
print()
print(df_dirty.head(5).to_string())
