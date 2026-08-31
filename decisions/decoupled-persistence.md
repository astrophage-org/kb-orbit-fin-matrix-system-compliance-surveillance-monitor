# ADR-006: Decoupled Persistence, Auditing, and Log Archival to BigQuery

## Status

**Accepted** — August 17, 2026

---

## Context

The Compliance Surveillance Monitor ([[summaries/project-astrophage]]) is designed to process high-throughput, high-density transactional message streams from the Global Financial Markets Group (GFMG) and SecureClear Financial Services (SCFS). These streams include matching engine executions (`nte.trades.matched`), book snapshots (`nte.orderbook.snapshots`), and post-trade settlements (`scfs.settlement.status`). 

Evaluating these events in real-time using complex analytical loops like the [[concepts/surveillance-stream-processor]] requires extremely low latency. Traditional streaming architectures often couple the stream processing pipeline directly to physical datastores (e.g., transactional relational databases, operational document stores) to maintain:
1. Active state (e.g., historical tracking for rules engine evaluations).
2. Diagnostic logging.
3. Alert auditing and regulatory reporting chains.

However, binding the runtime engine to inline databases introduces significant IO bottlenecks, transactional lock contention, and operational scaling constraints. In particular:
* **Throughput Penalties**: Direct synchronous database writes inside the real-time processing loop degrade processing guarantees and cause backpressure on upstream Kafka brokers.
* **Scaling Bottlenecks**: Tightly coupling operational stores limits horizontal scaling of worker nodes, as database connection pools and write capacities become the scaling constraint.
* **Regulatory Isolation**: Regulators require immutable, long-term archival of all compliance alerts and system state, spanning years. Operational databases optimized for short-term write throughput are ill-suited for massive cold storage, petabyte-scale historical audit queries, and complex offline analytics.

---

## Decision

We will completely decouple persistence, operational logging, and audit archiving from the real-time execution environment. All persistence of generated compliance alerts and operational metadata will be delegated asynchronously to **Google BigQuery** (`gfmg_surveillance.alerts_log`).

```
+-----------------------------------------------------------------------------------+
|                           STREAM PROCESSING ENVIRONMENT                           |
|                                                                                   |
|  [Incoming Streams] ---> [[concepts/surveillance-stream-processor]]               |
|                                 |                                                 |
|                                 v (In-Memory Processing Only)                     |
|                   +-------------+-------------+                                   |
|                   |                           |                                   |
|                   v                           v                                   |
|       [[concepts/wash-trade-detection]]  [[concepts/spoofing-detection]]          |
|                   |                           |                                   |
|                   +-------------+-------------+                                   |
|                                 |                                                 |
|                                 v (Alert Dispatched)                              |
|                   [Topic: gfmg.compliance.alerts]                                 |
+---------------------------------+-------------------------------------------------+
                                  |
                                  | (Asynchronous Consumption)
                                  v
+-----------------------------------------------------------------------------------+
|                             STORAGE & ANALYTICS LAYER                             |
|                                                                                   |
|                   [BigQuery Storage Write API Connector]                          |
|                                 |                                                 |
|                                 v (Streaming Inserts)                             |
|                    BigQuery Table: `alerts_log`                                   |
|                                                                                   |
+-----------------------------------------------------------------------------------+
```

### 1. Stateless Streaming Pipeline
The ingestion and analytical engines—specifically the [[concepts/surveillance-stream-processor]], [[concepts/wash-trade-detection]], and [[concepts/spoofing-detection]] subsystems—will remain strictly stateless or near-stateless. Intermediate processing state (e.g., active transaction graphs) will reside entirely in localized memory buffers and use strategies like [[concepts/stateless-graph-cleansing]] to prevent memory leaks. 

### 2. Asynchronous Transport Decoupling
Consistent with [[decisions/decoupling-analytics-transport]], any triggered alert or auditing event is immediately dispatched to a dedicated egress Kafka topic, `gfmg.compliance.alerts`. Under no circumstances will a worker node in the stream processor write directly to an external database during stream evaluation.

### 3. BigQuery Delegation
A decoupled consumer group (utilizing the high-performance BigQuery Storage Write API or a Kafka-to-BigQuery connector) will independently consume the `gfmg.compliance.alerts` topic, performing low-latency batch streaming inserts into the `gfmg_surveillance.alerts_log` BigQuery table. 

This establishes a clear separation:
* **The Hot Path** is managed by PySpark Structured Streaming and Kafka, keeping execution completely non-blocking.
* **The Cold Path / Cold Storage** is managed by Google BigQuery, providing native, managed, petabyte-scale analytics, partition tuning, and long-term durability.

### 4. Direct Schema Mapping
All structural event schemas generated by the matching and settlement components map transparently to dedicated schemas within the archival layer:
* [[entities/trade-matched-event]] instances that trigger alarms are logged with their nested buyer, seller, and transaction properties.
* [[entities/orderbook-snapshot-event]] metrics, such as L1–L5 values calculated by the [[concepts/orderbook-imbalance-formula]], are persisted to evaluate historical model drifts.
* [[entities/settlement-status-event]] and AML validations derived via the [[concepts/rules-engine-aml]] schema are archived for downstream compliance reviews.

### 5. Regulatory Audits and Off-line Replays
Since BigQuery maintains the historical record, validation of new machine learning models and threshold configurations (as defined in [[entities/aml-config]]) can be run entirely offline against historical BigQuery data. This completely avoids impacting production Kafka partitions, as detailed in [[decisions/decoupling-analytics-transport]].

---

## Consequences

### Positive Impacts
* **Uncapped Ingestion Performance**: Eliminating synchronous database operations ensures the [[concepts/surveillance-stream-processor]] can scale out horizontally, matching the performance of matching engines.
* **Cost Efficiency**: Cold-tier storage pricing in BigQuery is significantly cheaper than provisioning high-IOPS transactional databases for multi-year retention.
* **Enhanced Analytical Support**: Quantitative researchers can execute complex SQL queries, train new LSTM autoencoder models, or validate wash trade heuristics using years of historical graph snapshots directly inside BigQuery.
* **Operational Isolation**: System crashes, lock-escalations, or schema updates within BigQuery will never degrade, stall, or crash the real-time detection loops.

### Negative Impacts / Trade-offs
* **Eventual Consistency**: There is a micro-delay (typically 500ms to 2 seconds) between the emission of a compliance alert on the Kafka topic and its availability in BigQuery for historical search queries. High-priority real-time alerts must be handled directly via stream-consumers on `gfmg.compliance.alerts` rather than polling BigQuery.
* **Duplication Risks**: Due to the "at-least-once" delivery guarantees of the streaming ingestion pipeline, the write connectors may occasionally insert duplicate alert logs into BigQuery. Downstream audit interfaces must utilize deduplication queries leveraging unique alert IDs (`alert_id`) and timestamps.