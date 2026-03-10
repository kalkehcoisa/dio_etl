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

> **PS: O que é Churn?**
> Churn é quando um cliente abandona o relacionamento com uma empresa — no contexto bancário, significa fechar a conta ou migrar para outro banco. É uma métrica crítica para instituições financeiras, pois adquirir um novo cliente custa muito mais do que reter um existente. No dataset, `churn = 1` indica que o cliente já deixou o banco em algum período. São exatamente esses clientes que priorizamos para gerar mensagens de retenção, simulando uma ação de CRM.

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

## 📬 Amostra de Mensagens Geradas

Exemplos de mensagens personalizadas geradas pela IA para clientes em risco de churn:

| Nome | País | Saldo | Perfil | Mensagem |
|------|------|-------|--------|----------|
| Patrícia Rocha Alves | Germany | 131593.85 | cliente em risco de churn | Olá Patrícia, estamos aqui para ajudar e queremos garantir que você esteja completamente satisfeita com nossos serviços, por isso gostaríamos de oferecer uma revisão personalizada das suas contas e produtos para melhor atender às suas necessidades em Berlim. Você está disponível para uma breve conversa sobre como podemos melhorar nosso relacionamento e serviços para você?  |
| Silvana Costa Ferreira | France | 123711.73 | cliente em risco de churn | Silvana, sabemos que você tem um relacionamento importante com o ABC Bank e queremos garantir que você tenha a melhor experiência. Estamos aqui para ajudar com qualquer necessidade, seja no Brasil ou em Paris! |
| André Costa Araújo | Germany | 132405.52 | cliente em risco de churn | André, valorizamos muito sua confiança no ABC Bank. Que tal explorarmos juntos oportunidades para potencializar seu relacionamento conosco?  |
| Mariana Costa Santos | France | 238387.56 | cliente em risco de churn | Mariana, valorizamos muito sua confiança no ABC Bank e queremos garantir que você tenha a melhor experiência. Estamos aqui para ajudar com qualquer necessidade ou dúvida! |
| Paulo Roberto Mendes | Germany | 120100.41 | cliente em risco de churn | Paulo, valorizamos muito sua confiança no ABC Bank. Que tal aproveitar seu saldo e limite disponível para um investimento ou produto exclusivo? Estamos aqui para ajudar!
| Mariana Costa Santos | Germany | 103700.69 | cliente em risco de churn | Mariana, sabemos que você tem um saldo significativo no ABC Bank e queremos garantir que você tenha a melhor experiência. Estamos aqui para ajudar com qualquer necessidade ou dúvida!
| Marcos Pereira Rocha | France | 0.00 | cliente em risco de churn | Marcos, valorizamos muito sua confiança no ABC Bank. Estamos aqui para ajudar a encontrar a melhor solução para você!  |
| Vanessa Gomes Carvalho | Spain | 0.00 | cliente em risco de churn | Vanessa, sabemos que você é uma cliente importante para nós e adoraríamos continuar te ajudando com suas necessidades financeiras. Estamos aqui para oferecer soluções que se adaptem ao seu momento atual. |
| Paulo Roberto Mendes | Germany | 109339.17 | cliente em risco de churn | Paulo, valorizamos muito sua confiança no ABC Bank. Que tal conversarmos sobre como podemos melhorar sua experiência conosco?  |
| João Pedro Souza | Germany | 127655.22 | cliente em risco de churn | João Pedro, valorizamos muito sua confiança no ABC Bank. Que tal explorarmos juntos benefícios exclusivos para você?  |

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
