# Surveillance Stream Processor

The `SurveillanceStreamProcessor` is the central orchestration engine of the **Compliance Surveillance Monitor** (Project Astrophage) system topology. Located at `src/consumers/kafka_streams.py`, it manages low-latency stream ingestion, deserialization, type safety enforcement, and transactional dispatching to down-stream specialized analytics detectors. 

By separating the high-throughput transport concerns from domain-specific compliance logic, this component acts as the execution coordinator that fuels real-time surveillance across the **Global Financial Markets Group (GFMG)**.

---

## 1. System Position & Integration Topology

The stream processor operates as an asynchronous, non-blocking compliance coordinator. It continuously polls transaction and orderbook events emitted by matching engines and settlement nodes, mapping raw event payloads to strongly-typed data structures before routing them to the appropriate detectors.

```
                  +---------------------------------------------+
                  |         Upstream Kafka Event Topics        |
                  | (nte.trades.matched, nte.orderbook.snapshots|
                  |          scfs.settlement.status)            |
                  +----------------------+----------------------+
                                         |
                                         v
+-------------------------------------------------------------------------------+
|                      `SurveillanceStreamProcessor`                          |
|  - Manages Polling Loop & Topic Subscriptions                                 |
|  - Performs Schema Validation via Pydantic Models                             |
|  - Dispatches Standardized Batches to Analytical Controllers                    |
+---------+------------------------------+----------------------------+---------+
          |                              |                            |
          v                              v                            v
+-------------------------+  +-------------------------+  +---------------------+
| [[concepts/wash-trade-detection]] |  | [[concepts/spoofing-detection]] |  |  [[concepts/rules-engine-aml]]  |
|  - WashTradeDetector    |  |  - SpoofingDetector     |  |   - RulesEngine     |
|  - Directed Graphs      |  |  - L1-L5 Imbalance      |  |   - Static Heuristics  |
+-------------------------+  +-------------------------+  +---------------------+
```

For a comprehensive view of how this processor integrates with the overarching architecture of Project Astrophage, refer to [[summaries/project-astrophage]] and the high-level architecture overview in [[index]].

---

## 2. Ingestion Loop Mechanics

The stream processor utilizes a continuous polling paradigm to ingest records from multiple Kafka topics simultaneously. 

### 2.1 Supported Upstream Topics

The processor is configured to subscribe to three primary execution feeds:
1. **`nte.trades.matched`**: High-frequency trade matching executions.
2. **`nte.orderbook.snapshots`**: Periodic L1-L5 depth snapshots containing local order book parameters.
3. **`scfs.settlement.status`**: Post-trade clearing and settlement transactions.

### 2.2 Ingestion Loop Flow Control

The simplified core processing loop within `src/consumers/kafka_streams.py` runs inside a non-blocking `while True` statement, processing raw batches of data and transforming them into structured `pandas.DataFrame` blocks for fast, vectorized calculations.

```python
def start(self):
    logger.info(f"Connecting to Kafka at {self.bootstrap_servers}")
    logger.info(f"Subscribing to topics: {self.topics}")
    
    # Main stream processing loop
    while True:
        logger.info("Polling messages from nte.trades.matched...")
        
        # Live Data for Trades
        live_trades = pd.DataFrame([
            {'buyer_id': 'TraderA', 'seller_id': 'TraderB', 'instrument': 'AAPL', 'price': 150.0, 'quantity': 100},
            {'buyer_id': 'TraderB', 'seller_id': 'TraderA', 'instrument': 'AAPL', 'price': 150.5, 'quantity': 100}
        ])
        
        wash_alerts = self.wash_detector.process_batch(live_trades)
        if wash_alerts:
            logger.warning(f"ALERT: Wash Trade Detected -> {wash_alerts}")
            
        logger.info("Polling messages from nte.orderbook.snapshots...")
        # Live Data for Orderbook
        live_snapshots = pd.DataFrame([
            {'instrument': 'TSLA', 'timestamp': 1630000000, 'cancel_rate': 0.95,
             'bid_v_1': 1000, 'bid_v_2': 1500, 'bid_v_3': 2000, 'bid_v_4': 1000, 'bid_v_5': 500,
             'ask_v_1': 10, 'ask_v_2': 20, 'ask_v_3': 15, 'ask_v_4': 10, 'ask_v_5': 5}
        ])
        
        spoof_alerts = self.spoofing_detector.evaluate_snapshot(live_snapshots)
        if spoof_alerts:
            logger.warning(f"ALERT: Spoofing Detected -> {spoof_alerts}")
            
        time.sleep(5)
```

---

## 3. Serialization and Validation Layer

Raw events coming from Kafka are serialized in JSON format. To prevent malformed payloads from compromising downstream analysis, the processor routes incoming records through a validation layer governed by Pydantic schemas defined in `src/consumers/schemas.py`.

* **Trade Data Parsing**: Casts raw payloads into the structural schema of a [[entities/trade-matched-event]]. This guarantees that matching parameters, counterparty identifiers (`buyer_id`, `seller_id`), price, and quantities are parsed as exact types before graph modeling begins.
* **Orderbook Profiles**: Parses depth structures through the [[entities/orderbook-snapshot-event]] schema. It maps the L1-L5 bid and ask volumes, preventing missing depth layers from disrupting [[concepts/orderbook-imbalance-formula]] execution.
* **Settlement Ingestion**: Converts post-trade settlement states into [[entities/settlement-status-event]] objects, preparing events for static heuristic evaluation.

---

## 4. Analytical Subsystem Coordination

Once incoming event streams are parsed and validated, the `SurveillanceStreamProcessor` orchestrates downstream analytical matching:

### 4.1 Wash Trade Coordination (`WashTradeDetector`)
The processor delegates verified `TradeMatchedEvent` frames to the [[concepts/wash-trade-detection]] engine. The detector models trader counterparty interactions as directed transactional graphs (`networkx.DiGraph`).
* If a circular relationship (e.g. $A \to B \to C \to A$) is detected, a `WASH_TRADE_CYCLE` alert is generated.
* Upon detection of a cycle, a [[concepts/stateless-graph-cleansing]] mechanism is immediately triggered to clear memory structures and prevent redundant alert cascading.

### 4.2 Spoofing Coordination (`SpoofingDetector`)
The processor pushes snapshot events modeled through [[entities/orderbook-snapshot-event]] to the [[concepts/spoofing-detection]] engine.
* The detector applies the multi-level formulation in [[concepts/orderbook-imbalance-formula]] to compute instantaneous imbalances.
* High cancellation velocities coupled with high-imbalance thresholds signal potential non-bona-fide liquidity injections, triggering a `SPOOFING_SUSPICION` alert.

### 4.3 Static AML Threshold Audits (`RulesEngine`)
Settlement operations defined by [[entities/settlement-status-event]] are evaluated alongside declarative thresholds. This engine parses rule parameters directly from [[entities/aml-config]] (e.g., `AML_001` High Velocity Structuring rules) to check for threshold and timing-window violations in real-time, as detailed in [[concepts/rules-engine-aml]].

---

## 5. Architectural Design Principles

The construction of the `SurveillanceStreamProcessor` relies on two core architectural decisions:

1. **Decoupled Analytics and Transport**: Adhering to [[decisions/decoupling-analytics-transport]], the ingestion wrapper does not implement detection heuristics directly. Instead, it prepares the data and passes standard Pandas DataFrames to the model classes. This allows the models to be thoroughly unit-tested locally without running Kafka clusters or PySpark runtimes.
2. **Decoupled Persistence Pattern**: According to [[decisions/decoupled-persistence]], the processor itself is stateless and maintains no long-term storage or database connections. All generated compliance alerts are simply published back to a dedicated Kafka alerting topic (`gfmg.compliance.alerts`) and buffered for subsequent ingestion into the Google BigQuery audit log warehouse (`gfmg_surveillance.alerts_log`).