# Desafio técnico 02 — ETL Simples com Dados de Táxi de NYC

![Pipeline](https://raw.githubusercontent.com/TonFLY/desafio02_dadosportodos/master/assets/pipeline.png)

Dados por Todos - [Linkedin do Ecossistema](https://www.linkedin.com/company/dadosportodos/) 

Venha fazer parte do Ecossistema - [Formulário de inscrição](https://docs.google.com/forms/d/e/1FAIpQLSdbhBVbw9JbiRFXyiM60ufSes671v3p0YHqPqOQJh863Aysmg/viewform)

Fonte dos dados - [Kaggle](https://www.kaggle.com/datasets/elemento/nyc-yellow-taxi-trip-data)

---

## Objetivo

Construir uma pipeline de dados end-to-end utilizando dados de corridas de táxi de Nova York, cobrindo:

* ingestão de dados brutos (CSV)
* validação e rastreabilidade de qualidade
* transformação para camada analítica
* persistência em formato Parquet
* consumo via SQL (DuckDB)
* visualização com Streamlit

---

## Stack Tecnológica

* Python 3.14
* pandas
* pyarrow
* duckdb
* streamlit

---

## Dados

* Fonte: Kaggle — NYC Yellow Taxi Trip Data
* Tipo: Yellow Taxi
* Fonte dos dados em [Kaggle](https://www.kaggle.com/datasets/elemento/nyc-yellow-taxi-trip-data)
* Período:

  * Janeiro/2015
  * Janeiro–Março/2016

Os arquivos CSV devem ser armazenados em:

```bash
data/raw/
```

---

## Arquitetura do Projeto

```bash
├── src/                # Pipeline de dados (ETL/ELT)
├── data/
│   ├── raw/           # Dados brutos (CSV)
│   └── processed/     # Dados processados (Parquet)
├── sql/               # Queries e views analíticas
├── app/
│   └── dashboard.py   # Dashboard Streamlit
├── notebooks/         # Exploração (não faz parte da pipeline)
├── requirements.txt
└── README.md
```

---

## Fluxo da Pipeline

```text
CSV (raw)
   ↓
Ingestão (pandas)
   ↓
Validação de Qualidade (flags + score)
   ↓
Transformação (features e métricas)
   ↓
Camada Clean / Invalid
   ↓
Agregações analíticas
   ↓
Persistência (Parquet)
   ↓
Consumo via DuckDB (SQL)
   ↓
Dashboard (Streamlit)
```

---

## Como Executar

### 1. Criar ambiente virtual (Windows)

```powershell
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Executar pipeline

```powershell
python -m src.pipeline
```

Rodar com amostra:

```powershell
python -m src.pipeline --nrows 100000
```

### 3. Rodar dashboard

```powershell
streamlit run app/dashboard.py
```

---

## Artefatos Gerados

Após a execução, os seguintes arquivos serão gerados em `data/processed/`:

* taxi_clean.parquet
* taxi_invalid.parquet
* taxi_quality_summary.parquet
* taxi_quality_by_rule.parquet
* taxi_daily.parquet
* taxi_weekly.parquet
* taxi_shift.parquet
* taxi_vendor.parquet
* taxi_payment_type.parquet

---

## Descrição dos Datasets

### taxi_clean.parquet

Base validada e enriquecida, utilizada para análises.

Contém:

* colunas originais
* features derivadas (tempo, duração, velocidade, etc.)
* categorizações (turno, faixas)

---

### taxi_invalid.parquet

Registros considerados inválidos pelas regras de qualidade.

Inclui:

* flags (flag_*)
* qtd_regras_invalidas

Utilizado para diagnóstico e análise de qualidade.

---

### taxi_quality_summary.parquet

Resumo por lote (`arquivo_origem`):

* total de registros
* registros válidos e inválidos
* percentual de erro
* score de qualidade
* contagem por regra

Inclui linha global: `__GLOBAL__`

---

### taxi_quality_by_rule.parquet

Análise por regra de qualidade:

* regra
* invalid_count
* invalid_pct

Permite identificar os principais problemas de dados.

---

### taxi_daily.parquet

Métricas agregadas por dia:

* total_trips
* avg_distance
* avg_fare
* avg_duration
* avg_speed
* avg_tip_pct
* total_revenue

---

### taxi_weekly.parquet

Agregação semanal (`pickup_week_start`) com mesmas métricas.

---

### taxi_shift.parquet

Análise por turno:

* morning
* afternoon
* evening
* night
* unknown

---

### taxi_vendor.parquet

Análise por fornecedor do sistema (`VendorID`).

---

### taxi_payment_type.parquet

Análise por tipo de pagamento:

* credit card
* cash
* no charge
* dispute
* unknown
* voided trip

---

## Regras de Qualidade

As validações incluem:

* datas inválidas ou nulas
* ordem temporal incorreta
* distância inválida
* passageiros inválidos
* tarifa inválida
* duração inválida
* velocidade fora de faixa plausível
* vendor inválido
* payment_type inválido
* ratecode inválido
* inconsistência financeira
* duplicidade
* coordenadas fora de NYC
* registros fora do período esperado

Cada regra gera uma flag (`flag_*`) e compõe a flag consolidada:

```python
flag_invalido
```

---

## Dashboard

O dashboard em Streamlit consome os dados processados via DuckDB e apresenta:

* KPIs gerais
* séries temporais
* análise por vendor
* análise por tipo de pagamento
* qualidade dos dados

---

## Verificações de Entrega

* Pipeline executa sem erros
* Arquivos Parquet gerados corretamente
* Leitura com `pandas.read_parquet()` funcionando
* Dashboard carregando sem exceções
* Métricas e gráficos exibidos corretamente

---

## Notas Técnicas

* O campo `shift_of_day` é derivado em `transform.py`
* Corridas com `payment_type = 6` (Voided trip) são tratadas como inválidas
* Reexecute o pipeline após alterações nas regras:

```powershell
python -m src.pipeline
```

---

## Checklist de Submissão

* Código organizado e documentado
* Pipeline funcional
* Parquets gerados
* Dashboard operacional
* Regras de qualidade documentadas

---

## Próximos Passos

* integração com BigQuery ou Fabric
* orquestração com Airflow
* testes automatizados de qualidade
* versionamento de dados
* monitoramento de pipeline

---


