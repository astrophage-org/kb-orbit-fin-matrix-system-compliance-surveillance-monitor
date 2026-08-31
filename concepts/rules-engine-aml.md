# Declarative Threshold Validation and AML Compliance Matcher

The **AML Rules Engine** is a core sub-system of [[summaries/project-astrophage]]. It provides real-time, declarative evaluation of transactional events against regulatory compliance heuristics and anti-money laundering (AML) thresholds. 

By separating the rule definitions into a declarative schema—defined in [[entities/aml-config]]—the platform allows compliance teams to modify thresholds, velocity windows, and watchlist constraints without necessitating a recompilation or redeployment of the underlying [[concepts/surveillance-stream-processor]].

---

## 1. System Integration & Event Flow

The Rules Engine acts as a post-matching and post-settlement validation layer. It runs side-by-side with behavioral heuristics like the [[concepts/wash-trade-detection]] and [[concepts/spoofing-detection]] models, but focuses on deterministic threshold-based and entity-based checks.

```
                    [ Kafka Ingest Topics ]
                     /                 \
     (nte.trades.matched)           (scfs.settlement.status)
              |                                |
              v                                v
    [[entities/trade-matched-event]]  [[entities/settlement-status-event]]
              \                                /
               v                              v
             +----------------------------------+
             |        Rules Engine Matcher      | <--- [[entities/aml-config]]
             +----------------+-----------------+
                              |
                              v
               [ Consolidated Alert Dispatcher ]
              (gfmg.compliance.alerts / BigQuery)
```

---

## 2. Declarative Rule Configurations

All active rule criteria are configured using a JSON schema managed within [[entities/aml-config]]. The Rules Engine parses this configuration at startup (`src/main.py`) and maps each active rule to its respective stream partition.

### Key Rules Implemented

1. **High Velocity Structuring (`AML_001`)**
   * **Objective**: Detect transactions designed to deliberately bypass the standard \$10,000 regulatory reporting threshold (e.g., structuring multiple transfers of \$9,900).
   * **Source Topic**: `scfs.settlement.status` (using the [[entities/settlement-status-event]] schema).
   * **Heuristic**: Evaluates total transacted amounts per participant within a sliding 24-hour window, triggering when the cumulative count of sub-threshold transactions exceeds a predefined frequency limit.

2. **Sanctioned Entity Interaction (`AML_002`)**
   * **Objective**: Intercept settlement or trading activity involving individuals, corporations, or wallets flagged on international sanction regimes (e.g., OFAC lists).
   * **Source Topic**: `nte.trades.matched` (using the [[entities/trade-matched-event]] schema).
   * **Heuristic**: Performs low-latency hash lookups on `buyer_id` and `seller_id` against active blacklists.

---

## 3. Mathematical & Algorithmic Mechanics

### 3.1 Velocity Tracking (AML_001)

The system tracks velocity-based structuring using a sliding-window aggregation. Let $T_p$ be the set of settlement events for a given participant $p$ within a sliding temporal window $W$ (defined by `time_window_hours`):

$$T_p = \{ t_i \mid \text{timestamp}(t_i) \ge t_{\text{current}} - W \}$$

An alert is generated if both of the following conditions are met:

$$\forall t_i \in T_p, \quad \text{amount}(t_i) < \text{max\_amount\_usd}$$

$$\text{Count}(T_p) \ge \text{count\_limit}$$

This strategy isolates sequences of transactions that individually escape regulatory scrutiny but collectively represent suspicious velocity patterns.

### 3.2 Sanction Lookup (AML_002)

To preserve microsecond-level processing guarantees, the watchlist matcher processes incoming [[entities/trade-matched-event]] profiles by evaluating:

$$f(\text{buyer\_id}, \text{seller\_id}) = \mathbb{I}(\text{buyer\_id} \in \mathcal{S}) \lor \mathbb{I}(\text{seller\_id} \in \mathcal{S})$$

Where:
* $\mathcal{S}$ is the in-memory hash set of sanctioned identifiers.
* $\mathbb{I}$ is the indicator function returning $1$ if a match occurs, prompting an immediate `HIGH` severity AML alert.

---

## 4. Architectural Principles

### 4.1 Separation of Concerns
Following [[decisions/decoupling-analytics-transport]], the validation logic of the rules engine is transport-agnostic. The core evaluation rules accept raw collections or DataFrames (e.g., Pandas, PySpark), making them fully unit-testable offline without active connection to Confluent Kafka brokers.

### 4.2 Decoupled Persistence & Auditing
When an validation failure occurs, the rules engine avoids inline database transactions. Instead, it dispatches an event to the `gfmg.compliance.alerts` Kafka stream. Following [[decisions/decoupled-persistence]], these events are ingested asynchronously by downstream consumers and appended to Google BigQuery (`gfmg_surveillance.alerts_log`) for long-term auditability.

---

## See Also
* [[index]]
* [[summaries/project-astrophage]]
* [[entities/aml-config]]
* [[concepts/surveillance-stream-processor]]