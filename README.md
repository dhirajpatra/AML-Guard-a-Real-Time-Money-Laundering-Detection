# AML-Guard — Real-Time Money Laundering Detection

A containerised, observable, multi-agent system that scores every transaction in real time against a
**Neo4j knowledge graph** and an **AML ontology**, and escalates suspicious activity to a **LangGraph**
team of agents that investigates and explains it. The LLM is **pluggable** (Ollama by default).

> **Status:** Phase 2 of 6 complete (hot-path stream processor with live verdicts, plus an evaluation benchmark).
> See [Roadmap](#roadmap). This is a reference implementation on **synthetic data** — not a
> production-certified AML system.

---

## 1. Design principles

1. **Split by latency.** An LLM cannot sit in a millisecond path, so detection runs in three tiers:

   | Tier | Latency | Runs | Purpose |
   |---|---|---|---|
   | Hot path | ~20–200 ms | Rules, Redis velocity features, targeted Cypher, light ML score | Verdict on every transaction: `PASS` / `REVIEW` / `BLOCK` |
   | Warm path | ~3–15 s | LangGraph multi-agent investigation | Only for flagged/ambiguous transactions; evidence, typology, narrative |
   | Cold path | scheduled | Neo4j GDS (Louvain, PageRank, cycles) | Network risk features fed back into the hot path |

2. **LLM independence.** All agent code talks to one `get_llm()` factory driven by `LLM_PROVIDER`,
   `LLM_MODEL`, `LLM_BASE_URL`, `LLM_API_KEY`. Switching Ollama → OpenAI/Anthropic/any
   OpenAI-compatible server is an `.env` change, never a code change. (Implemented in Phase 4.)
3. **No label leakage.** The simulator publishes ground truth to a *separate* topic
   (`transactions.truth`) used only for evaluation. Shell/mule status is **never** written to the graph;
   detection must infer it from structure (shared address/device, jurisdiction, flows).
4. **Agents use whitelisted tools.** Parameterised Cypher tools only — no free-form query generation.
5. **Everything is observable and auditable.** Traces, metrics, logs, LLM traces, immutable audit trail.

## 2. Architecture

```
 simulator ──► Redpanda (transactions.raw) ──► stream-processor ──► decisions / alerts topics
    │               │                              │   │  ▲
    │ truth         │                              │   │  └─ Redis (velocity, sanctions cache)
    ▼               ▼                              ▼   ▼
 transactions.truth                             Neo4j (KG + GDS)      Postgres (alerts, cases, audit)
 (evaluation only)                                  ▲                        ▲
                                                    └──── agent-service (LangGraph) ◄── get_llm() ──► Ollama | API
 api (FastAPI) ──► raw topic          dashboard UI                │
 OpenTelemetry ─► Prometheus / Tempo / Loki / Grafana / Langfuse ◄┘
```

## 3. Knowledge graph & ontology

**Entities:** `Customer` (person/organization), `Account`, `Transaction`, `Device`, `Address`, `Jurisdiction`.
**Relationships:** `OWNS`, `LIVES_AT`, `USES_DEVICE`, `RESIDENT_IN`, and (Phase 2)
`(Account)-[:SENT]->(Transaction)-[:RECEIVED_BY]->(Account)`.

**Ontology layer** (`graph/01_schema.cypher`):
- `OntologyClass` hierarchy (`Party → Person | Organization → ShellCompany`). *ShellCompany is inferred, never asserted.*
- `Typology` nodes, each linked to the `RiskIndicator`s that evidence it.

| Typology | Indicators | Simulated by |
|---|---|---|
| `STRUCTURING` | just-below-threshold, high velocity | 5–9 transfers of 9,000–9,900 |
| `FAN_IN` | many-to-one, shared device, velocity | 8–12 mule accounts → one collector |
| `ROUND_TRIP` | cycle, layered hops | 3–4 account ring returning to origin |
| `RAPID_PASS_THROUGH` | pass-through, high-risk jurisdiction | in → out within 3–15 s, minus ~2% |
| `SHELL_LAYERING` | layered hops, shared address, high-risk jurisdiction | 4–5 shell chain ending in a high-risk country |

## 4. Repository layout

```
aml-guard/
├── README.md
├── docker-compose.yml          # all services, with profiles (llm, tools)
├── .env.example                # copy to .env
├── Makefile                    # make help
├── pytest.ini
├── graph/
│   └── 01_schema.cypher        # constraints, indexes, ontology, typologies (idempotent)
├── db/postgres/init.sql        # decisions, alerts, audit_log
├── libs/aml_common/            # shared settings, event models, JSON logging
├── services/
│   ├── simulator/              # Phase 1: synthetic transactions + laundering scenarios
│   │   └── simulator/{population,scenarios,graph_seed,main,settings}.py
│   └── stream_processor/       # Phase 2: the real-time hot path
│       └── stream_processor/
│           ├── main.py         # Kafka loop: raw -> decisions / alerts / DLQ, Postgres batches
│           ├── pipeline.py     # one transaction: Redis -> graph -> score
│           ├── features.py     # Redis sliding windows (velocity, fan-in, structuring, pass-through)
│           ├── graph.py        # Neo4j: ontology, profiles, flow tracing (Cypher), storage
│           ├── rules.py        # indicators -> typology scores -> verdict (pure, unit-tested)
│           ├── ontology.py     # typology fallback (checked against graph/01_schema.cypher)
│           ├── persistence.py  # idempotent Postgres writer
│           └── evaluator.py    # precision / recall / latency report
├── tests/{simulator,stream_processor}/   # unit tests + end-to-end "digital twin" test
├── requirements-dev.txt
└── docs/
```
Planned: `services/agent_service/` (P4), `services/api/` & `ui/` (P6),
`observability/` (P5).

## 5. Prerequisites

- Docker Engine 24+ with the Compose v2 plugin (`docker compose version`)
- **8 GB RAM free** for the core stack; **+6–8 GB** (and ideally a GPU) if you run Ollama with an 8B model
- Free ports: 7474, 7687, 19092, 9644, 6379, 5432 (+ 11434 with `llm`, 8080 with `tools`)
- Optional for local tests: Python 3.11+

## 6. Installation & quick start

```bash
git clone <your-repo> aml-guard && cd aml-guard
make init                 # creates .env from .env.example (edit passwords if you like)
make up                   # core: Neo4j, Redpanda, Redis, Postgres, simulator
make ps                   # wait until everything is healthy / init jobs exited 0
```
First start takes a few minutes (image pulls; Neo4j downloads the APOC + GDS plugins, so it needs internet).

With a local LLM (needed from Phase 4):
```bash
make up-llm               # adds Ollama and pulls $OLLAMA_PULL_MODEL (llama3.1:8b, ~5 GB)
```
Optional Kafka UI: `make up-all` → http://localhost:8080

Without `make`: `cp .env.example .env && docker compose up -d --build`
(add `--profile llm` / `--profile tools` as needed).

## 7. Verifying the stack (Phases 1–2)

1. **Containers:** `make ps` → `neo4j`, `redpanda`, `redis`, `postgres`, `simulator` running;
   `neo4j-init` and `redpanda-init` exited with code 0.
2. **Simulator logs:** `make logs s=simulator` → JSON lines: `population`, `graph seeded`,
   `scenario started` (with typology), periodic `stats`.
3. **Stream:** `make peek` (raw transactions) and `make truth` (labels, e.g. `"typology":"FAN_IN"`).
4. **Graph:** open http://localhost:7474 (user `neo4j`, password from `.env`), then:
   ```cypher
   MATCH (t:Typology)-[:INDICATED_BY]->(i) RETURN t.id, collect(i.id);        // ontology
   MATCH (a:Address)<-[:LIVES_AT]-(c) WITH a, count(c) AS n WHERE n > 1
   RETURN a.address_id, n ORDER BY n DESC;                                    // shared-address clusters
   MATCH (j:Jurisdiction {high_risk:true}) RETURN j.code;                     // high-risk list
   ```
5. **Hot path (Phase 2):** `make logs s=stream-processor` shows `stats` lines (tps, latency percentiles, review/block counts);
   `make decisions` / `make alerts` show verdicts; in Neo4j:
   ```cypher
   MATCH (a:Account)-[f:TRANSFERRED_TO]->(b) RETURN a.account_id, b.account_id, f.count, f.last_amount LIMIT 10;
   ```
   Postgres: `docker compose exec postgres psql -U aml -c "select verdict, count(*) from decisions group by 1"`
6. **Unit tests (host):** `pip install -r requirements-dev.txt && make test`

## 8. Phase 2: how the hot path decides

For every message on `transactions.raw` the processor does, in order:

1. **Redis** (one pipelined round trip): event-time sliding windows — sender velocity, distinct senders into
   the receiver (fan-in), near-threshold count (structuring), amounts the sender just received (pass-through).
2. **Neo4j** (one transaction, *read-then-write*): for material amounts it traces **flow-conserving paths**
   over `(Account)-[:TRANSFERRED_TO]->(Account)` edges — each hop later than the last, within `FLOW_WINDOW_S`,
   and carrying 80–102% of the previous amount. A ring closed by this transaction = `CYCLE`; a 2–3 hop chain
   ending at the sender = `LAYERED_HOPS`. Then the transaction is stored (idempotent `MERGE`).
   Account profiles (shared address/device peers) and the high-risk jurisdiction list are cached in-process.
3. **Indicators fire** with a strength (0–1) — e.g. `CYCLE` 1.0, `MANY_TO_ONE` 0.9, `JUST_BELOW_THRESHOLD` 0.8,
   `HIGH_RISK_JURISDICTION` 0.7, `SHARED_ADDRESS` 0.35.
4. **Ontology scoring:** the processor loads each `Typology` and its `RiskIndicator`s from Neo4j.
   `typology score = base_weight × noisy-OR(strengths of its fired indicators)`; the highest wins.
5. **Verdict:** `BLOCK` ≥ 0.80, `REVIEW` ≥ 0.50, else `PASS`. Weak signals alone (a busy account, a shared
   address) never alert; strong or corroborated evidence does.

The decision (verdict, score, typology, indicators, evidence, latencies) is published to
`transactions.decisions`; `REVIEW`/`BLOCK` also go to `alerts` (Phase 4 agents consume this) and Postgres
(batched, idempotent, off the verdict path). Unparseable messages go to `transactions.dlq`.
Offsets are committed only after decisions are flushed → at-least-once, safe on replay.

**Known limits (deliberate for now):** a pattern is flagged on the transaction that *completes* it (the first
hops of a ring look innocent); typology labels on intermediate hops can be ambiguous (agents resolve this in
Phase 4); one processor instance handles tens to low hundreds of tps — scale by running more replicas
(topics have 3 partitions); `TRANSFERRED_TO` edges are not pruned yet (Phase 3 adds time-bucketing).

### Benchmark recipe (detection quality + latency)

```bash
make reset                                                   # clean volumes
SIM_MAX_TXNS=6000 make up                                    # finite, reproducible run
make logs s=stream-processor                                 # watch p50/p95/p99 in the `stats` lines
make eval                                                    # joins decisions with transactions.truth
```
`make eval` prints txn-level precision/recall/FPR, per-typology scenario detection, and latency percentiles.
`processing_ms` is time inside the hot path; `e2e_ms` additionally includes queueing.
After upgrading from Phase 1 run `make reset` (the simulator population changed, so the old graph is stale).

## 9. Configuration (`.env`)

| Variable | Default | Meaning |
|---|---|---|
| `NEO4J_USER` / `NEO4J_PASSWORD` | `neo4j` / `aml_password_123` | Graph credentials (password ≥ 8 chars) |
| `KAFKA_BOOTSTRAP` | `redpanda:9092` | Broker (in-network). Host tools use `localhost:19092` |
| `LLM_PROVIDER` | `ollama` | `ollama` \| `openai` \| `anthropic` \| `openai_compatible` |
| `LLM_MODEL` | `llama3.1:8b` | Model id for the chosen provider (use a **tool-calling** model) |
| `LLM_BASE_URL` / `LLM_API_KEY` | ollama URL / empty | Endpoint and key (not needed for Ollama) |
| `OLLAMA_PULL_MODEL` | `llama3.1:8b` | Model pulled by the `ollama-pull` job |
| `SIM_TPS` | `10` | Normal transactions per second |
| `SIM_CUSTOMERS` | `2000` | Population size (min 100) |
| `SIM_SCENARIOS_PER_MIN` | `6` | Laundering scenarios started per minute |
| `SIM_SEED` | `42` | RNG seed (reproducible runs) |
| `SIM_MAX_TXNS` | `0` | Stop after N transactions (0 = forever) |
| `HIGH_RISK_COUNTRIES` | `IR,KP,MM` | Jurisdictions flagged high-risk in the graph |
| `REVIEW_THRESHOLD` / `BLOCK_THRESHOLD` | `0.50` / `0.80` | Risk score cut-offs for `REVIEW` / `BLOCK` |
| `FLOW_WINDOW_S` | `300` | How far back flow tracing looks (seconds) |
| `FLOW_MIN_AMOUNT` | `5000` | Graph flow queries run only for material amounts (keeps latency low) |
| `PROC_OFFSET_RESET` | `earliest` | Where a new processor group starts reading `transactions.raw` |

Run a finite, reproducible burst: `SIM_MAX_TXNS=5000 SIM_SEED=7 docker compose up -d --build simulator`.

## 10. Everyday commands

| Command | Does |
|---|---|
| `make up` / `make up-llm` / `make up-all` | Start core / + Ollama / + Kafka UI |
| `make down` | Stop, keep data |
| `make reset` | Stop and **delete all volumes** (fresh graph, topics, DB) |
| `make logs s=<service>` | Tail logs |
| `make sim-stop` / `make sim-start` | Pause / resume traffic |
| `make decisions` / `make alerts` | Peek at hot-path verdicts / alerts |
| `make eval` | Precision, recall and latency report vs ground truth |
| `make graph-shell` | cypher-shell into Neo4j |

## 11. Switching the LLM (Phase 4 design, config already in place)

```bash
# Local (default)
LLM_PROVIDER=ollama   LLM_MODEL=qwen2.5:7b   LLM_BASE_URL=http://ollama:11434
# Hosted
LLM_PROVIDER=openai   LLM_MODEL=gpt-4o-mini  LLM_API_KEY=sk-...
# Any OpenAI-compatible server (vLLM, LM Studio, LiteLLM proxy…)
LLM_PROVIDER=openai_compatible  LLM_BASE_URL=http://host:8000/v1  LLM_MODEL=...
```
Then `docker compose up -d agent-service`. Agents never import a provider SDK directly.

## 12. Roadmap

| Phase | Scope | Status |
|---|---|---|
| 1 | Compose skeleton, graph schema + ontology, simulator, tests | ✅ done |
| 2 | Hot path: stream processor, rules, Redis features, graph write + flow tracing, evaluator | ✅ done |
| 3 | GDS batch jobs (communities, cycles, centrality) feeding hot-path features | next |
| 4 | LangGraph multi-agent system, tools, `get_llm()` factory, human-in-the-loop | |
| 5 | Observability: OpenTelemetry, Prometheus, Grafana, Tempo, Loki, Langfuse | |
| 6 | API, dashboard UI, case management, evaluation report (precision/recall/latency) | |

## 13. Troubleshooting

- **Neo4j never becomes healthy:** it must reach the internet to fetch plugins on first boot; check
  `docker compose logs neo4j`. Heap/pagecache are set low (1 GB / 512 MB) — raise in compose for bigger runs.
- **`simulator` restarts:** it waits up to 60 s for Neo4j; check `docker compose logs simulator`.
- **No decisions appear:** check `make logs s=stream-processor`; it waits for Neo4j, Redis and Postgres.
  Verdicts only flow once `transactions.raw` has data (`make peek`).
- **Everything is `PASS`:** flow tracing needs amounts ≥ `FLOW_MIN_AMOUNT` and a stack that ran after `make reset`.
- **Port already in use:** change the left side of the `ports:` mapping in `docker-compose.yml`.
- **Auth failure after changing the password:** the old password is stored in the volume → `make reset`.
- **Ollama is slow:** use a smaller model, or enable the GPU block in `docker-compose.yml`.

## 14. Disclaimer

Synthetic data only. Thresholds, typologies and scores are illustrative and unvalidated; real AML
programmes need regulatory review, model governance, and real KYC/sanctions data sources.
