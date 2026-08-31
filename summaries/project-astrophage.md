<!-- anchor: docs/architecture.md:L1-L100 sha:HEAD -->

# Architectural Summary: Compliance Surveillance Monitor (Project Astrophage)

The **Compliance Surveillance Monitor** (Project Astrophage) is the real-time, high-throughput Regulatory Surveillance Platform designed for the **Global Financial Markets Group (GFMG)** and its clearing subsidiary, **SecureClear Financial Services (SCFS)**. This system acts as an asynchronous, non-blocking compliance layer, consuming high-density market events to detect fraudulent activities, wash trading, spoofing, and other market manipulations in real-time.

For a broader view of the enterprise systems, refer to the [[index]].

---

## 1. System Topology & Context

The platform functions as an asynchronous, non-blocking compliance layer. It consumes high-density event streams emitted by the core exchange matching engines and settlement systems, evaluates transactions against analytical models, and dispatches alerts to dedicated downstream topics and archival systems.

```
+---------------------------------+
|      Order Matching Engine      |
|  (nte.trades.matched, etc.)     |
+----------------+----------------+
                 |
                 | (Kafka Event Streams)
                 v
+----------------+----------------+
|  Compliance Surveillance Monitor| <--- Rules Config ([[entities/aml-config]])
|  (This System)                  |
+----------------+----------------+
                 |
                 | (Publish Alerts)
                 v
+----------------+----------------+
|   gfmg.compliance.alerts (Kafka)|
|   BigQuery Alerts Log           |
+---------------------------------+
```

---

## 2. Component & Subsystem Decomposition

The platform is structured modularly to decouple stream ingestion mechanics from specific analytical detectors and declarative configurations.

```
                       +----------------------------------+
                       |           main.py                |
                       |  (Bootstraps & loads Configs)    |
                       +----------------+-----------------+
                                        |
                                        v
                       +----------------+-----------------+
                       | [[concepts/surveillance-stream-processor]] |
                       |  (Kafka Ingestion Loop / Spark)  |
                       +-------+------------------+-------+
                               |                  |
              +----------------+                  +---------------+
              |                                                   |
              v                                                   v
+-------------+--------------+                      +-------------+--------------+
| [[concepts/wash-trade-detection]] |               | [[concepts/spoofing-detection]] |
|  - Graph Cycle Analysis    |                      |  - Orderbook Imbalance     |
|  - NetworkX Integration    |                      |  - LSTM Autoencoder (Prod) |
+----------------------------+                      +----------------------------+
```

### 2.1 Ingestion Layer ([[concepts/surveillance-stream-processor]])
* **Source Reference**: `src/consumers/kafka_streams.py`
* **Responsibility**: Manages low-latency consumption of financial message streams. It acts as the driver loop, fetching batches from multiple Kafka topics, standardizing payloads into robust internal models, and dispatching tasks to specialized analytical modules.
* **Execution Environment**: Wraps scalable streaming components (such as **PySpark Structured Streaming** and **Confluent Kafka** consumer groups) to support horizontal scaling.

### 2.2 Graph-Based Wash Trade Subsystem ([[concepts/wash-trade-detection]])
* **Source Reference**: `src/models/wash_trade_detector.py`
* **Responsibility**: Detects circular transaction networks (e.g., Trader $A \to B \to C \to A$) within localized time windows, targeting synthetic volume inflation and illicit price manipulation.
* **Implementation**: Uses `networkx` to dynamically build directed transactional graphs. Market participants are treated as nodes, while transactions are mapped as directed edges carrying trade metadata. To manage memory leakages and prevent stale dependencies, it implements dynamic buffer flushing via [[concepts/stateless-graph-cleansing]].

### 2.3 Market Microstructure Spoofing Subsystem ([[concepts/spoofing-detection]])
* **Source Reference**: `src/models/spoofing_detector.py`
* **Responsibility**: Analyzes orderbook imbalances and high cancellation velocities near the spread to detect spoofing activity.
* **Implementation**: Analyzes Level 1 through Level 5 (L1–L5) orderbook depth profiles using [[concepts/orderbook-imbalance-formula]]. The structural interface is designed to support deep-learning model extensions (such as LSTM Autoencoders) for multi-dimensional anomaly detection.

### 2.4 Rules Engine & AML Processor ([[concepts/rules-engine-aml]])
* **Source Reference**: `rules/aml_config.json`
* **Responsibility**: Evaluates static regulatory checks, anti-money laundering thresholds, and sanctions list queries against structured event streams based on declarative schemas configured in [[entities/aml-config]].

---

## 3. Data Pipelines & Processing Flow

The ingest pipelines process incoming records dynamically depending on the event schema:

```
                    [ Kafka Ingestion Topics ]
                    /           |            \
                   /            |             \
(nte.trades.matched)   (nte.orderbook.snapshots)  (scfs.settlement.status)
        |                       |                         |
        v                       v                         v
  Pydantic Model          Pydantic Model            Pydantic Model
[[entities/trade-matched-event]] [[entities/orderbook-snapshot-event]] [[entities/settlement-status-event]]
        |                       |                         |
        v                       v                         v
 [ [[concepts/wash-trade-detection]] ] [ [[concepts/spoofing-detection]] ]  [ [[concepts/rules-engine-aml]] ]
  (Graph Analysis)      (Imbalance Formula)        (Threshold Matcher)
        \                       |                         /
         \                      |                        /
          v                     v                       v
               [ Consolidated Alert Dispatcher ]
                (gfmg.compliance.alerts / BigQuery)
```

### 3.1 Wash Trading Detection Pipeline
1. **Consumption**: Reads raw payloads from matching engines (`nte.trades.matched`).
2. **Parsing & Validation**: Normalizes the stream using the [[entities/trade-matched-event]] schema.
3. **Graph Execution**: Constructs a transient directed graph (`nx.DiGraph`) out of buyer and seller identifiers within [[concepts/wash-trade-detection]].
4. **Heuristic Evaluation**: Executes a cycle detection search (`nx.find_cycle`).
5. **Alert Emission**: Generates a `WASH_TRADE_CYCLE` alert containing graph metadata, involved counterparty nodes, and event timestamps.

### 3.2 Spoofing Detection Pipeline
1. **Consumption**: Monitors book updates via (`nte.orderbook.snapshots`).
2. **Parsing & Validation**: Validates payloads against the [[entities/orderbook-snapshot-event]] schema.
3. **Metric Calculation**: Measures instantaneous orderbook imbalance along with order cancellation frequencies using [[concepts/orderbook-imbalance-formula]].
4. **Threshold Guarding**: Evaluates calculations against preset imbalance and velocity limits in [[concepts/spoofing-detection]].
5. **Alert Emission**: Outputs a `SPOOFING_SUSPICION` event outlining book depth statistics and confidence metrics.

---

## 4. Key Abstractions & Data Models

### 4.1 Strongly-Typed Event Schemas (`src/consumers/schemas.py`)
Data modeling relies on Pydantic to maintain schema safety across the microservice boundary:
* [[entities/trade-matched-event]]: Models execution properties including transaction ID, execution price, volume, trading counterparties, and timestamps.
* [[entities/orderbook-snapshot-event]]: Models bid/ask arrays up to depth L5, alongside cancellation rates.
* [[entities/settlement-status-event]]: Tracks clearance states (`PENDING`, `SETTLED`, `FAILED`) for post-trade AML pipelines.

### 4.2 Declarative Rule Configurations ([[entities/aml-config]])
Operational rules are defined declaratively within `aml_config.json` to decouple compliance adjustments from redeployments:
* **Structuring Checks (AML_001)**: Flag actions designed to bypass the standard \$10,000 regulatory reporting threshold using [[concepts/rules-engine-aml]].
* **Sanction Enforcement (AML_002)**: Instantly alerts on activity from entities flagged on OFAC watchlists.

---

## 5. Architectural Implementation Principles

### 5.1 Decoupling Analytics from Transport
The stream consumers are fully separated from the analytical execution models. This architectural choice is guided by [[decisions/decoupling-analytics-transport]]. This isolation ensures that detectors can be unit-tested using deterministic inputs (such as Pandas DataFrames) without a running PySpark container or physical connection to a Kafka cluster.

### 5.2 Orderbook Imbalance Formulation
The system calculates orderbook depth imbalance using multi-level bid-ask spreads defined in [[concepts/orderbook-imbalance-formula]]. This balance calculation reveals underlying buy/sell pressures, highlighting potential manipulative forces when combined with high cancellation rates.

### 5.3 Stateless Graph Cleansing
To balance performance and accuracy, [[concepts/wash-trade-detection]] purges its active memory buffers immediately after a cycle is successfully found. The engineering choices behind this pattern are documented under [[concepts/stateless-graph-cleansing]]. This mitigates duplicate alert cascades, stabilizes memory footprints, and guarantees low CPU overhead across long-running streams.

### 5.4 Decoupled Persistence Design
Alert payloads are published asynchronously back to Kafka (`gfmg.compliance.alerts`) and archived to Google BigQuery (`gfmg_surveillance.alerts_log`). This keeps the real-time processing engine stateless, delegating cold storage, indexing, and audits to the data warehouse, as outlined in [[decisions/decoupled-persistence]].