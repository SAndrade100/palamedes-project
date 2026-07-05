# Palamedes — Framework de Performability Empírica

> Framework Python de linha de comando (CLI) para automação do ciclo de vida completo de experimentos de **performability** — avaliação integrada de desempenho e dependabilidade em ambientes de contêineres.

---

## Sumário

1. [Visão Geral](#1-visão-geral)
2. [Arquitetura](#2-arquitetura)
3. [Pré-requisitos e Instalação](#3-pré-requisitos-e-instalação)
4. [Uso Rápido](#4-uso-rápido)
5. [Comandos CLI](#5-comandos-cli)
6. [Referência da DSL YAML](#6-referência-da-dsl-yaml)
7. [Ciclo de Vida do Experimento](#7-ciclo-de-vida-do-experimento)
8. [Drivers de Carga](#8-drivers-de-carga)
9. [Injetores de Falhas](#9-injetores-de-falhas)
10. [Telemetria e Métricas](#10-telemetria-e-métricas)
11. [Análise e Artefatos de Saída](#11-análise-e-artefatos-de-saída)
12. [Execução em Lote (Batch)](#12-execução-em-lote-batch)
13. [Testes](#13-testes)
14. [Estrutura do Projeto](#14-estrutura-do-projeto)
15. [Stack Tecnológica](#15-stack-tecnológica)

---

## 1. Visão Geral

O Palamedes automatiza experimentos que cruzam **injeção de carga massiva** com **injeção controlada de falhas de infraestrutura**, medindo o impacto de falhas na qualidade de serviço (QoS). Diferente de ferramentas de benchmarking ou chaos engineering isoladas, ele atua na interseção das duas áreas e fecha o ciclo de feedback ao gerar dados empíricos estruturados para validação de modelos analíticos (Cadeias de Markov, Redes de Petri).

**O que o framework faz automaticamente:**

1. Orquestra e valida a infraestrutura Docker sob teste
2. Aplica carga de trabalho até atingir regime permanente (*steady-state*)
3. Registra a linha de base nominal (vazão e latência sem falhas)
4. Dispara a falha de forma síncrona e cronometrada (ou por gatilho reativo)
5. Monitora a resiliência e o tempo de autorrecuperação
6. Calcula métricas de dependabilidade e exporta relatórios

---

## 2. Arquitetura

### Visão em camadas

```
┌─────────────────────────────────────────────────────────┐
│  CLI  (Typer + Rich)                                    │
│  palamedes run / batch / validate / report              │
├─────────────────────────────────────────────────────────┤
│  Core                                                   │
│  ┌─────────────────┐  ┌──────────┐  ┌───────────────┐  │
│  │  Orchestrator   │  │   FSM    │  │   Scheduler   │  │
│  │  (asyncio loop) │  │ (estados)│  │ (gatilhos)    │  │
│  └────────┬────────┘  └──────────┘  └───────┬───────┘  │
│           │ coordena                         │ dispara  │
├───────────┼──────────────────────────────────┼──────────┤
│  Drivers  │          Injectors               │          │
│  ┌────────┴──┐      ┌──────────────────────┐ │          │
│  │  asyncio  │      │ container / network  │◄┘          │
│  │  k6       │      │ resource (stress-ng) │            │
│  └─────┬─────┘      └──────────┬───────────┘            │
│        │ métricas              │ eventos                │
├────────┼───────────────────────┼────────────────────────┤
│  Telemetria                    │                        │
│  SoftwareCollector + InfraCollector → DuckDB            │
├─────────────────────────────────────────────────────────┤
│  Analytics                                              │
│  metrics.py (MTRS, atenuação) + exporter + plotter      │
└─────────────────────────────────────────────────────────┘
```

### Máquina de estados (FSM)

O estado de cada experimento é controlado por uma FSM com eventos `asyncio.Event`, garantindo transições seguras e permitindo que qualquer corrotina aguarde uma fase específica:

```
IDLE → SETUP → WARMUP → BASELINE → FAULT_INJECTION → RECOVERY → TEARDOWN → DONE
              ↘         ↘          ↘                 ↘          ↘
                                      ERROR → TEARDOWN
```

### Concorrência

O Orchestrator executa três grupos de tarefas em paralelo via `asyncio`:

| Tarefa | O que faz |
|---|---|
| Driver de carga | Gera requisições HTTP continuamente |
| `SoftwareMetricsCollector` | Poll do driver a cada `collection_interval_ms` |
| `InfraMetricsCollector` | Poll do Docker Stats API + psutil (thread pool) |
| `TelemetryCollector.run_flush_loop` | Flush em batch para DuckDB a cada 1s |
| `Scheduler` | Aguarda gatilho temporal ou reativo e dispara a falha |

---

## 3. Pré-requisitos e Instalação

### Dependências do sistema

| Dependência | Obrigatório | Observação |
|---|---|---|
| Python 3.11+ | ✅ Sempre | |
| Docker Engine | ✅ Sempre | Socket acessível pelo usuário |
| `uv` | Recomendado | Alternativa: `pip` |
| `k6` | Somente driver k6 | Disponível em [k6.io](https://k6.io) |
| `stress-ng` | Somente falhas de recurso | Deve estar dentro do container alvo |
| `iproute2` (`tc`) | Somente falhas de rede | Deve estar dentro do container alvo + `NET_ADMIN` |

### Instalação

```bash
# Clone o repositório
git clone <repo-url>
cd palamedes-project

# Instale com uv (recomendado)
pip install uv
uv sync

# Ou com pip
pip install -e ".[dev]"
```

Verifique a instalação:

```bash
palamedes --help
```

---

## 4. Uso Rápido

```bash
# 1. Valide seu arquivo de configuração
palamedes validate examples/redis_failover.yaml

# 2. Execute um experimento simples
palamedes run examples/redis_failover.yaml

# 3. Execute com varredura de parâmetros
palamedes batch examples/redis_failover.yaml

# 4. Regenere relatórios de uma execução anterior
palamedes report results/redis-failover-01/metrics.duckdb --output-dir relatorios/
```

---

## 5. Comandos CLI

### `palamedes run <config.yaml>`

Executa um experimento completo. Ao final, exibe a tabela de métricas de dependabilidade e exporta todos os artefatos.

```
Opções:
  --results-dir, -o   Diretório de saída (padrão: results/)
  --verbose, -v       Ativa logs DEBUG
  --no-export         Pula a exportação de artefatos
```

### `palamedes batch <config.yaml>`

Executa a varredura de parâmetros definida na seção `[batch]` do config, gerando N × M execuções automáticas.

### `palamedes validate <config.yaml>`

Valida o arquivo YAML contra o schema Pydantic sem executar nada. Útil para verificar a sintaxe antes de submeter um experimento longo.

### `palamedes report <metrics.duckdb>`

Regenera gráficos e exporta artefatos a partir de uma execução anterior sem re-rodar o experimento.

```
Opções:
  --output-dir, -o   Diretório de saída dos arquivos
```

### `palamedes export-schema <output.json>`

Exporta o JSON Schema completo da DSL YAML para documentação ou validação em IDEs.

---

## 6. Referência da DSL YAML

### Estrutura completa

```yaml
experiment:
  id: meu-experimento-01          # [obrigatório] alfanumérico + hífens/underscores
  description: "Texto livre"      # [opcional]

  target:
    compose_file: ./docker-compose.yml  # [opcional]
    container: nome-do-container        # [obrigatório] nome exato no Docker
    service: nome-do-servico            # [opcional]

  phases:
    warmup:
      duration_seconds: 60              # duração máxima do aquecimento
      steady_state:                     # [opcional] detecta regime permanente
        metric: throughput_rps          # campo de MetricSnapshot
        min_value: 500.0                # valor mínimo aceitável
        stability_window_seconds: 10    # deve sustentar por N segundos consecutivos
    baseline:
      duration_seconds: 120             # duração da coleta da linha de base
    recovery_timeout_seconds: 300       # timeout máximo aguardando recuperação

  load:
    driver: asyncio                     # "asyncio" ou "k6"
    config: { ... }                     # parâmetros específicos do driver (ver seção 8)

  fault:
    type: container_stop                # tipo da falha (ver seção 9)
    target_container: nome-container
    trigger:
      type: temporal                    # "temporal" ou "reactive"
      offset_seconds: 30               # [temporal] segundos após início da fase
      # --- OU ---
      # type: reactive
      # metric: cpu_percent
      # threshold: 85.0
      # comparator: gt                 # gt | lt | gte | lte
    duration_seconds: 60               # 0 = falha permanente
    parameters: { }                    # parâmetros extras por tipo (ver seção 9)

  sla:
    max_error_rate_percent: 1.0
    max_p99_latency_ms: 500.0

  metrics:
    collection_interval_ms: 500        # frequência de coleta (mínimo: 100ms)
    software:
      - throughput_rps
      - p95_latency_ms
      - p99_latency_ms
      - error_rate_percent
    infra:
      - cpu_percent
      - memory_percent
      - network_bytes_sent
      - network_bytes_recv

batch:                                  # [opcional] varredura de parâmetros
  parameter_sweep:
    parameter: load.config.arrival_rate_rps   # caminho dotted dentro de experiment
    values: [50.0, 100.0, 200.0, 400.0]
  repeat: 3                             # repetições por valor
```

---

## 7. Ciclo de Vida do Experimento

| Fase | Duração | O que acontece |
|---|---|---|
| **SETUP** | ~2s | Driver de carga sobe, conexões são estabelecidas. Evento: `experiment_start`, `setup_complete` |
| **WARMUP** | Até `warmup.duration_seconds` | Carga aplicada; aguarda regime permanente se `steady_state` configurado. Evento: `warmup_start`, `steady_state_reached` |
| **BASELINE** | `baseline.duration_seconds` | Métricas nominais coletadas sem perturbação. Evento: `baseline_start`, `baseline_complete` |
| **FAULT_INJECTION** | Aguarda gatilho + `duration_seconds` | Scheduler dispara a falha. Evento: `fault_injected`, `degradation_detected` |
| **RECOVERY** | Até `recovery_timeout_seconds` | Monitor aguarda p99 e error_rate voltarem dentro do SLA (3 leituras consecutivas OK). Evento: `recovery_start`, `recovery_complete` |
| **TEARDOWN** | Instantâneo | Injector é restaurado; DuckDB recebe flush final. Evento: `teardown_start`, `experiment_end` |

Cada evento recebe um timestamp de precisão em milissegundos registrado na tabela `events` do DuckDB.

---

## 8. Drivers de Carga

### Driver `asyncio` (recomendado)

Gerador de carga nativo Python usando `aiohttp` com processo de chegada Poisson. Oferece controle total sobre a temporização e rastreamento por requisição.

```yaml
load:
  driver: asyncio
  config:
    target_url: http://localhost:8080/endpoint
    method: GET          # GET, POST, PUT, DELETE, etc.
    arrival_rate_rps: 100.0   # taxa λ do processo de Poisson
```

**Como funciona:** O intervalo entre chegadas segue `Exp(1/λ)` usando `numpy.random.exponential`. Cada requisição é disparada como uma `asyncio.Task` independente. As métricas são calculadas sobre uma janela deslizante de 5 segundos.

### Driver `k6`

Executa o `k6` como subprocesso e consome seu output JSON em streaming.

```yaml
load:
  driver: k6
  config:
    script: ./scripts/meu-teste.js   # script k6 existente
    vus: 100                          # virtual users iniciais
    ramp:                             # [opcional] estágios de rampa
      - duration: "30s"
        target: 50
      - duration: "60s"
        target: 100
```

**Requisito:** `k6` instalado e disponível no PATH.

---

## 9. Injetores de Falhas

### Falhas de contêiner

Controladas via Docker SDK Python. Todas são **revertidas automaticamente** na fase de TEARDOWN.

| `type` | Ação | Parâmetros `parameters` |
|---|---|---|
| `container_stop` | `docker stop` — para o container graciosamente | `timeout: 10` (segundos) |
| `container_pause` | `docker pause` — congela todos os processos | — |
| `container_kill` | `docker kill` — envia sinal UNIX | `signal: SIGKILL` |

### Falhas de rede

Injetadas via `tc-netem` dentro do container alvo com `docker exec`. Requerem `iproute2` instalado no container e capability `NET_ADMIN`.

| `type` | Efeito | Parâmetros `parameters` |
|---|---|---|
| `network_latency` | Latência artificial em todas as saídas | `latency_ms: 200`, `jitter_ms: 30`, `interface: eth0` |
| `network_loss` | Descarte aleatório de pacotes | `loss_percent: 10`, `interface: eth0` |
| `network_partition` | 100% de perda — simula partição de rede | `interface: eth0` |

**Comando executado internamente:**
```bash
tc qdisc add dev eth0 root netem delay 200ms 30ms distribution normal
```

### Falhas de recurso

Executadas via `stress-ng` com `docker exec`. Requerem `stress-ng` instalado no container alvo.

| `type` | Efeito | Parâmetros `parameters` |
|---|---|---|
| `cpu_stress` | N workers em loop de CPU | `workers: 2` |
| `memory_stress` | N workers alocando memória RAM | `workers: 1`, `vm_bytes: 256M` |

O `stress-ng` se encerra automaticamente após `duration_seconds`. Se `restore()` for chamado antes, os processos são terminados com `pkill`.

---

## 10. Telemetria e Métricas

### Métricas coletadas

| Métrica | Origem | Descrição |
|---|---|---|
| `throughput_rps` | Driver de carga | Vazão atual (req/s) na janela de 5s |
| `p50_latency_ms` | Driver de carga | Latência mediana |
| `p95_latency_ms` | Driver de carga | Latência no percentil 95 |
| `p99_latency_ms` | Driver de carga | Latência no percentil 99 (cauda) |
| `error_rate_percent` | Driver de carga | % de respostas com status >= 500 ou timeout |
| `cpu_percent` | Docker Stats API | Uso de CPU do container alvo |
| `memory_percent` | Docker Stats API | Uso de memória do container alvo |
| `network_bytes_sent` | psutil | Bytes enviados pelo host |
| `network_bytes_recv` | psutil | Bytes recebidos pelo host |

### Schema do DuckDB

Todas as métricas e eventos são armazenados em `results/<id>/metrics.duckdb`:

```sql
-- Série temporal de métricas
SELECT * FROM metrics WHERE phase = 'FAULT_INJECTION' ORDER BY ts_ms;

-- Timeline de eventos com timestamp em ms
SELECT event_type, ts_ms, phase FROM events ORDER BY ts_ms;

-- Throughput médio por fase
SELECT phase, AVG(throughput_rps), AVG(p99_latency_ms)
FROM metrics GROUP BY phase ORDER BY MIN(ts_ms);
```

---

## 11. Análise e Artefatos de Saída

### Métricas de dependabilidade calculadas automaticamente

| Métrica | Fórmula | Interpretação |
|---|---|---|
| **MTRS** *(Mean Time to Restore Service)* | `ts(recovery_complete) − ts(fault_injected)` | Tempo total de restauração do serviço em ms |
| **Janela de Indisponibilidade** | Σ intervalos com `error_rate > SLA` ou `p99 > SLA` | Período total em que o SLA foi violado |
| **Índice de Atenuação de Desempenho** | `(baseline_tp − fault_min_tp) / baseline_tp × 100` | % de queda de vazão durante a falha |

### Artefatos gerados em `results/<id>/`

| Arquivo | Formato | Uso |
|---|---|---|
| `metrics.duckdb` | DuckDB | Série temporal completa; queryável via SQL ou Python |
| `<id>_metrics.csv` | CSV | Análise em Excel, pandas, R |
| `<id>_events.csv` | CSV | Timeline de eventos para análise de fase |
| `<id>_result.json` | JSON | Resultado estruturado com métricas de dependabilidade |
| `<id>_metrics.parquet` | Parquet | Análise em pandas/polars/Spark |
| `<id>_timeline.pdf` | PDF | Gráfico vetorial para visualização |
| `<id>_timeline.pgf` | PGF/TikZ | Figura para inclusão direta em artigos LaTeX |
| `<id>_timeline.html` | HTML | Relatório interativo com Plotly |

### Usando os dados em Python

```python
import duckdb
import polars as pl

conn = duckdb.connect("results/redis-failover-01/metrics.duckdb", read_only=True)

# Série temporal completa
df = conn.execute("SELECT * FROM metrics ORDER BY ts_ms").pl()

# Comparar baseline vs falha
resumo = conn.execute("""
    SELECT phase,
           AVG(throughput_rps)   AS avg_tp,
           AVG(p99_latency_ms)   AS avg_p99,
           AVG(error_rate_percent) AS avg_err
    FROM metrics
    GROUP BY phase
    ORDER BY MIN(ts_ms)
""").pl()

print(resumo)
conn.close()
```

### Incluindo o gráfico em LaTeX

```latex
\usepackage{pgf}

\begin{figure}[h]
    \centering
    \input{figures/redis-failover-01_timeline.pgf}
    \caption{Desempenho × Tempo com marcador do instante da falha.}
    \label{fig:redis-failover}
\end{figure}
```

---

## 12. Execução em Lote (Batch)

A seção `batch` permite varredura de parâmetros automática (análise de sensibilidade):

```yaml
batch:
  parameter_sweep:
    parameter: load.config.arrival_rate_rps  # qualquer campo dentro de experiment
    values: [50.0, 100.0, 200.0, 400.0]
  repeat: 3
```

Isso gera **4 valores × 3 repetições = 12 execuções** com IDs únicos no formato:

```
redis-failover-01__arrival_rate_rps_50.0__r1
redis-failover-01__arrival_rate_rps_50.0__r2
redis-failover-01__arrival_rate_rps_50.0__r3
redis-failover-01__arrival_rate_rps_100.0__r1
...
```

Cada execução tem seu próprio diretório em `results/` com DuckDB e artefatos independentes. Ao final o CLI exibe uma tabela comparativa com MTRS e atenuação por execução.

**Exemplos de parâmetros para sweep:**

```yaml
parameter: load.config.arrival_rate_rps    # taxa de carga
parameter: fault.duration_seconds          # duração da falha
parameter: fault.parameters.latency_ms     # latência de rede
parameter: phases.baseline.duration_seconds
```

---

## 13. Testes

### Executar testes unitários

```bash
# Todos os testes unitários (não requerem Docker)
uv run pytest tests/unit/ -v

# Com cobertura de código
uv run pytest tests/unit/ --cov=palamedes --cov-report=term-missing
```

### O que é testado

| Arquivo | Cobertura |
|---|---|
| `tests/unit/test_config.py` | Parsing YAML válido, rejeição de configs inválidas, overrides de parâmetros |
| `tests/unit/test_state_machine.py` | Todas as transições válidas e inválidas, `wait_for` assíncrono, reset |
| `tests/unit/test_metrics.py` | MTRS, atenuação de desempenho, janela de indisponibilidade com DuckDB real |

---

## 14. Estrutura do Projeto

```
palamedes-project/
│
├── pyproject.toml                  # Dependências e configuração do projeto
├── README.md                       # Esta documentação
│
├── palamedes/                      # Pacote principal
│   ├── __init__.py
│   │
│   ├── cli/
│   │   └── main.py                 # Typer CLI: run, batch, validate, report, export-schema
│   │
│   ├── config/
│   │   ├── schema.py               # Modelos Pydantic v2 — DSL YAML completo
│   │   └── loader.py               # Parser YAML → validação + apply_parameter_override
│   │
│   ├── models/
│   │   ├── events.py               # ExperimentPhase, EventType, ExperimentEvent, EventTimeline
│   │   └── experiment.py           # MetricSnapshot, PhaseRecord, DependabilityMetrics, ExperimentResult
│   │
│   ├── core/
│   │   ├── state_machine.py        # FSM com asyncio.Event por fase
│   │   ├── orchestrator.py         # Loop principal: coordena todas as tarefas asyncio
│   │   ├── scheduler.py            # Gatilhos temporais e reativos
│   │   └── batch_runner.py         # Varredura de parâmetros (parameter sweep)
│   │
│   ├── drivers/
│   │   ├── base.py                 # Protocol LoadDriver (start/stop/get_metrics/set_target_rps)
│   │   ├── asyncio_http.py         # aiohttp + processo de Poisson via numpy
│   │   └── k6.py                   # Subprocesso k6 + parser de output JSONL
│   │
│   ├── injectors/
│   │   ├── base.py                 # Protocol FaultInjector (inject/restore/verify_injected)
│   │   ├── container.py            # docker-py: stop / pause / kill
│   │   ├── network.py              # tc-netem via docker exec (latência, perda, partição)
│   │   └── resource.py             # stress-ng via docker exec (CPU, memória)
│   │
│   ├── telemetry/
│   │   ├── collector.py            # DuckDB streaming: buffer → flush em batch
│   │   ├── software.py             # Poll do driver de carga → TelemetryCollector
│   │   └── infra.py                # Docker Stats API + psutil → TelemetryCollector
│   │
│   └── analytics/
│       ├── metrics.py              # MTRS, janela de indisponibilidade, atenuação (polars + DuckDB)
│       ├── exporter.py             # CSV / JSON / Parquet a partir do DuckDB
│       └── plotter.py              # matplotlib PDF+PGF e Plotly HTML
│
├── examples/
│   ├── redis_failover.yaml         # Container parado com gatilho temporal
│   ├── nginx_network_latency.yaml  # Latência de rede no nginx
│   └── worker_cpu_stress.yaml      # CPU stress com gatilho reativo
│
└── tests/
    ├── unit/
    │   ├── test_config.py
    │   ├── test_state_machine.py
    │   └── test_metrics.py
    └── integration/                # Testes com Docker (a implementar)
```

---

## 15. Stack Tecnológica

| Camada | Tecnologia | Versão mínima |
|---|---|---|
| CLI | [Typer](https://typer.tiangolo.com/) + [Rich](https://rich.readthedocs.io/) | 0.12 / 13.0 |
| Validação de config | [Pydantic v2](https://docs.pydantic.dev/) + PyYAML | 2.0 / 6.0 |
| Controle de contêineres | [docker-py SDK](https://docker-py.readthedocs.io/) | 7.0 |
| Geração de carga | [aiohttp](https://docs.aiohttp.org/) + [NumPy](https://numpy.org/) | 3.9 / 1.26 |
| Armazenamento de métricas | [DuckDB](https://duckdb.org/) | 1.0 |
| Análise de dados | [Polars](https://pola.rs/) + [SciPy](https://scipy.org/) | 0.20 / 1.12 |
| Visualização | [matplotlib](https://matplotlib.org/) + [Plotly](https://plotly.com/python/) | 3.8 / 5.20 |
| Métricas de host | [psutil](https://psutil.readthedocs.io/) | 5.9 |
| Gerenciador de pacotes | [uv](https://docs.astral.sh/uv/) | — |


