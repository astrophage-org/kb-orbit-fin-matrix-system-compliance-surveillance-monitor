# Decoupling Analytics Transport and Execution Models

## Status

**Approved**

---

## Context

The Compliance Surveillance Monitor ([[summaries/project-astrophage]]) must evaluate high-density financial market events with sub-second latency. The stream ingestion loop, driven by the [[concepts/surveillance-stream-processor]], interfaces with real-time stream topologies (such as PySpark Structured Streaming and Confluent Kafka).

Historically, stream processing systems suffer from a high degree of architectural coupling. When analytical logic (such as cycle detection or mathematical formula evaluation) is written directly within the stream consumer framework, the following issues occur:
1. **Testing Friction**: Executing unit tests requires mock Kafka brokers, active JVM/Spark environments, or complex containerized scaffolding. This significantly increases local feedback loops and slows down CI/CD pipelines.
2. **Backtesting Difficulties**: Evaluating a new model against historical data dumps requires running the entire streaming infrastructure, rather than executing the algorithm directly over offline parquet files or database exports.
3. **Impeded ML Iteration**: Quantitative researchers and ML engineers are forced to understand transport-specific APIs (e.g., Spark structured streams, PySpark SQL schemas, or Kafka consumer poll configurations) simply to modify detection heuristics like the [[concepts/orderbook-imbalance-formula]].

To solve this, a strict boundary must be defined between the **Transport / Ingestion Layer** and the **Analytical / Execution Layer**.

---

## Decision

We will completely decouple the real-time event-streaming transport framework from the underlying analytical algorithms ([[concepts/wash-trade-detection]] and [[concepts/spoofing-detection]]) and rules engines ([[concepts/rules-engine-aml]]). 

```
+---------------------------------------------------------------------------------------------------+
|                                          TRANSPORT LAYER                                          |
|                                                                                                   |
|  [Kafka Streams]  --->  `SurveillanceStreamProcessor`  --->  Schema Validation                     |
|                               (Driver Loop / PySpark)          - `TradeMatchedEvent`             |
|                                                                - `OrderbookSnapshotEvent`        |
|                                                                - `SettlementStatusEvent`         |
+---------------------------------------------------------------------------------------------------+
                                                  |
                                                  | Translates validated events to standard 
                                                  | tabular payloads (e.g., pandas.DataFrame)
                                                  v
+---------------------------------------------------------------------------------------------------+
|                                         EXECUTION LAYER                                           |
|                                                                                                   |
|   +----------------------------+  +----------------------------+  +---------------------------+   |
|   |    `WashTradeDetector`   |  |     `SpoofingDetector`   |  |     `RulesEngine`       |   |
|   |  - Directed Graph cycles   |  |  - Orderbook Imbalance     |  |  - Threshold evaluations  |   |
|   |  - NetworkX Graph          |  |  - Machine Learning Model  |  |  - Uses [[entities/aml-config]] |   |
|   +----------------------------+  +----------------------------+  +---------------------------+   |
|                                                                                                   |
|                      (All execution components are 100% offline-testable)                         |
+---------------------------------------------------------------------------------------------------+
```

### 1. Unified Interface Boundary
The streaming consumer ([[concepts/surveillance-stream-processor]]) is solely responsible for network polling, cluster connection management, consumer offsets, and initial deserialization into strongly-typed Pydantic schemas:
* `[[entities/trade-matched-event]]`
* `[[entities/orderbook-snapshot-event]]`
* `[[entities/settlement-status-event]]`

Once verified, the processor converts micro-batches of these validated events into generic tabular structures—specifically standard **Pandas DataFrames**—before invoking the execution models.

### 2. Transport-Agnostic Execution Models
The analytical models have no reference to, import of, or dependency on `pyspark`, `confluent-kafka`, or any other transport system. 

* **[[concepts/wash-trade-detection]] (`WashTradeDetector`)**: Receives a `pandas.DataFrame` representing matched trades. It maintains a localized directed graph via `networkx` to locate cyclic paths and triggers [[concepts/stateless-graph-cleansing]] post-detection.
* **[[concepts/spoofing-detection]] (`SpoofingDetector`)**: Receives a `pandas.DataFrame` representing Level 1 to Level 5 depth profiles. It evaluates the [[concepts/orderbook-imbalance-formula]] and runs statistical/ML anomalies using standard `numpy` and `pandas` arithmetic.
* **[[concepts/rules-engine-aml]] (`RulesEngine`)**: Evaluates static declarative configurations parsed from `[[entities/aml-config]]` against settlement status tables, fully independent of stream state.

### 3. Support for Seamless Offline Testing
By keeping the execution models completely decoupled, developers can run fast, infrastructure-free offline tests. The following test script demonstrates how a mock dataset can be evaluated against the `WashTradeDetector` without launching any streaming dependencies:

```python
# test_wash_trade_detector_offline.py
import pandas as pd
from models.wash_trade_detector import WashTradeDetector

def test_offline_wash_trade_detection():
    # 1. Arrange: Construct offline mock data matching [[entities/trade-matched-event]] properties
    mock_trades = pd.DataFrame([
        {'buyer_id': 'Trader_A', 'seller_id': 'Trader_B', 'instrument': 'BTCUSD', 'price': 60000.0, 'quantity': 1.5},
        {'buyer_id': 'Trader_B', 'seller_id': 'Trader_C', 'instrument': 'BTCUSD', 'price': 60010.0, 'quantity': 1.5},
        {'buyer_id': 'Trader_C', 'seller_id': 'Trader_A', 'instrument': 'BTCUSD', 'price': 60005.0, 'quantity': 1.5}
    ])
    
    # 2. Act: Initialize and run detector offline
    detector = WashTradeDetector()
    alerts = detector.process_batch(mock_trades)
    
    # 3. Assert: Verify cyclic analysis logic worked
    assert len(alerts) == 1
    assert alerts[0]['type'] == 'WASH_TRADE_CYCLE'
    assert 'Trader_A' in alerts[0]['entities']
    print("Offline verification successful! No active Kafka cluster required.")
```

---

## Consequences

### Benefits
* **Sub-Second Test Suites**: Core logical routines, ML models, and heuristics can be evaluated in standard unit tests in milliseconds rather than minutes.
* **Streamlined Model Training & Backtesting**: Quantitative researchers can feed millions of historical records directly into `SpoofingDetector` from local parquet stores to backtest model thresholds.
* **Architectural Portability**: If the underlying stream engine is migrated (e.g., transitioning from Confluent Kafka Streams to Flink or custom Rust consumers), the entire analytical logic suite remains untouched.
* **Cohesive Alignment with Persistence Strategy**: By keeping compute logic isolated, we easily parallelize calculations and defer cold-path logging, indexing, and audits to BigQuery as defined in [[decisions/decoupled-persistence]].

### Liabilities
* **Serialization Overhead**: Translating streaming micro-batches into Pandas DataFrames adds a tiny CPU and memory serialization overhead. However, the performance cost is negligible compared to the processing latency of complex graph traversals or deep-learning model inferences.
* **State Management Boundary**: State tracking (e.g., maintaining historical windows across micro-batches) must either be self-contained within the detector object (e.g., class attributes) or explicitly managed by the coordinator layer, requiring developers to be careful with memory leaks on long-running worker threads. This is mitigated by implementing automated strategies like [[concepts/stateless-graph-cleansing]].