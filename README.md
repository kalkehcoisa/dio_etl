# 🚀 ETL com IA Generativa em Python

**DIO - Data Engineering | Santander Dev Week**

Pipeline completo de ETL que utiliza múltiplas IAs Generativas para gerar mensagens personalizadas de retenção para clientes bancários com risco de churn.

---

## 📋 Sobre o Projeto

Este projeto foi desenvolvido como parte do bootcamp de Data Engineering da [DIO](https://dio.me), com foco no fluxo ETL (**Extract → Transform → Load**) aplicado a dados bancários reais.

O dataset base é o [ABC Multistate Bank Customer Churn](https://www.kaggle.com/datasets/gauravtopre/bank-customer-churn-dataset) do Kaggle, enriquecido com campos extras e ruídos artificiais para simular um ambiente de produção real. A IA Generativa atua na etapa de **Transformação**, gerando mensagens personalizadas para cada cliente em risco de churn.

```
📥 EXTRACT                🔧 TRANSFORM                        📤 LOAD
bank_churn_dirty.csv  →  Limpeza + Enriquecimento + IA Gen  →  bank_churn_clean.csv
```

---

## 📁 Estrutura do Projeto

```
etl-bank-churn/
│
├── bank_churn_dirty.csv        # Dataset com ruídos artificiais (fonte do ETL)
├── etl_ia_generativa.ipynb     # Notebook principal com o pipeline completo
├── enriquecer_bank_churn.py    # Script que gerou o dataset sujo a partir do Kaggle
├── requirements.txt            # Dependências do projeto
├── env.example                 # Modelo do arquivo de variáveis de ambiente
└── README.md
```

### Arquivos gerados pelo pipeline
```
├── bank_churn_clean.csv        # Dataset completo após limpeza
└── bank_churn_mensagens.csv    # Clientes em risco + mensagem gerada pela IA
```

---

## 🗃️ Dataset

O dataset `bank_churn_dirty.csv` é uma versão enriquecida e intencionalmente "suja" do dataset original do Kaggle, criada para prática de ETL.

### Campos originais
| Campo | Descrição |
|---|---|
| `customer_id` | Identificador único do cliente |
| `credit_score` | Score de crédito |
| `country` | País (France, Spain, Germany) |
| `gender` | Gênero |
| `age` | Idade |
| `tenure` | Anos como cliente do banco |
| `balance` | Saldo em conta |
| `products_number` | Número de produtos contratados |
| `credit_card` | Possui cartão de crédito (1/0) |
| `active_member` | Membro ativo (1/0) |
| `estimated_salary` | Salário estimado |
| `churn` | **Target**: 1 = deixou o banco, 0 = permaneceu |

### Campos adicionados
| Campo | Descrição |
|---|---|
| `nome` | Nome completo gerado conforme o gênero |
| `email` | Email derivado do nome |
| `numero_conta` | Número de conta no formato XXXXX-X |
| `data_nascimento` | Data de nascimento derivada da idade |
| `cidade` | Cidade compatível com o país |
| `limite_credito` | Limite de crédito derivado do saldo |

### Ruídos injetados
| Tipo | Quantidade |
|---|---|
| Nomes em MAIÚSCULO | ~811 |
| Emails sem `@` | ~484 |
| Emails nulos | ~405 |
| Saldos negativos | ~316 |
| Saldos outlier (999999.99) | ~204 |
| Idades inválidas (-1) | ~304 |
| Credit score impossível (999) | ~303 |
| Salários nulos | ~605 |
| customer_id nulos | ~203 |
| Datas em formato misto | ~4054 |
| Cidades em minúsculo | ~814 |
| Linhas duplicadas | ~150 |

---

## 🔧 Pipeline ETL

### 📥 Extract
Leitura do `bank_churn_dirty.csv` e diagnóstico completo de qualidade — nulos, brancos e inconsistências em todas as colunas.

### 🔧 Transform

**Parte A — Limpeza e Padronização**
- Remoção de linhas duplicadas
- Brancos convertidos para `NaN` em todo o dataset
- Nomes padronizados (strip + title case)
- Emails inválidos, nulos e brancos → `sem_email@banco.com`
- Cidades padronizadas (strip + title case)
- Datas em formato misto → `DD/MM/YYYY`
- Idades inválidas (-1) → mediana
- Credit score impossível (999) → mediana
- Saldos negativos → 0 | Outliers → mediana
- Salário nulo → mediana
- customer_id nulo → novo ID sequencial

**Parte B — Classificação de Perfil**

Cada cliente é classificado antes da geração das mensagens para enriquecer o contexto enviado à IA:

| Perfil | Critério |
|---|---|
| cliente em risco de churn | `churn == 1` |
| cliente premium | `balance >= 100.000` |
| cliente inativo | `balance < 50.000` e `active_member == 0` |
| cliente com saldo zerado | `balance == 0` |
| cliente ativo | demais casos |

**Parte C — IA Generativa**

Para cada cliente com `churn = 1`, uma mensagem personalizada de retenção é gerada via IA, com base no perfil financeiro do cliente.

### 📤 Load
Exportação para dois arquivos CSV:
- `bank_churn_clean.csv` — dataset completo limpo (~10.000 registros)
- `bank_churn_mensagens.csv` — clientes em risco com mensagem da IA (~2.037 registros)

---

## 🤖 Estratégia de IA Generativa — Round-Robin com Cooldown

Com ~2.037 clientes para processar, o rate limit das APIs gratuitas seria um gargalo. A solução implementada distribui as requisições entre **4 provedores de IA** com uma lógica de cooldown inteligente:

- Cada provedor tem seu próprio timer de cooldown
- Ao receber erro `429`, o tempo de espera é extraído diretamente da mensagem de erro da API
- O provedor entra em cooldown pelo tempo exato indicado e o próximo disponível é usado **imediatamente**, sem espera
- Somente se **todos** os provedores estiverem em cooldown simultâneo o pipeline aguarda — e apenas pelo tempo do que liberar primeiro

### Provedores utilizados
| Provedor | Modelo |
|---|---|
| [Groq](https://console.groq.com) | `llama-3.3-70b-versatile` |
| [Google Gemini](https://aistudio.google.com) | `gemini-2.0-flash` |
| [Mistral](https://console.mistral.ai) | `mistral-small-latest` |
| [OpenRouter](https://openrouter.ai) | `meta-llama/llama-3.3-70b-instruct:free` |

---

## ⚙️ Como Executar

### 1. Clone o repositório
```bash
git clone https://github.com/seu-usuario/etl-bank-churn
cd etl-bank-churn
```

### 2. Crie e ative o ambiente virtual
```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 3. Instale as dependências
```bash
pip install -r requirements.txt
```

### 4. Configure as chaves de API
Copie o arquivo de exemplo e preencha com suas chaves:
```bash
cp env.example .env
```

```env
GROQ_API_KEY=sua-chave-aqui
GEMINI_API_KEY=sua-chave-aqui
MISTRAL_API_KEY=sua-chave-aqui
OPENROUTER_API_KEY=sua-chave-aqui
```

### 5. Suba o JupyterLab, abra o notebook e execute os blocos de código
```bash
jupyter-lab
```

---

## 📦 Dependências

```
pandas
numpy
python-dotenv
groq
google-genai
mistralai
openai
ipywidgets
```

---

## 🔗 Referências

- [Dataset original — Kaggle](https://www.kaggle.com/datasets/gauravtopre/bank-customer-churn-dataset)
- [DIO — Digital Innovation One](https://dio.me)
- [Groq Console](https://console.groq.com)
- [Google AI Studio](https://aistudio.google.com)
- [Mistral Console](https://console.mistral.ai)
- [OpenRouter](https://openrouter.ai)
