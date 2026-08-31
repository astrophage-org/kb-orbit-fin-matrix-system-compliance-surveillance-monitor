# Orderbook Imbalance & Spoofing Detection

The **Spoofing Subsystem** in [[summaries/project-astrophage|Project Astrophage]] is designed to detect market microstructure manipulation—specifically spoofing—by analyzing real-time orderbook imbalances and rapid, high-frequency cancellation patterns near the spread.

Spoofing is a manipulative practice where traders submit non-bona-fide orders (orders they do not intend to execute) to create a false impression of supply or demand, enticing other participants to trade at artificial prices. Once those trades occur, the spoofing orders are rapidly cancelled before execution.

---

## 1. System Architecture & Data Flow

The spoofing detector operates within the real-time event pipeline driven by the [[concepts/surveillance-stream-processor|Surveillance Stream Processor]]. 

```
                                  +------------------------------------+
                                  |      nte.orderbook.snapshots       |
                                  +-----------------+------------------+
                                                    |
                                                    v [Raw Kafka Stream]
                                  +-----------------+------------------+
                                  | `SurveillanceStreamProcessor`    |
                                  +-----------------+------------------+
                                                    |
                                                    v [Pydantic Validation]
                                  +-----------------+------------------+
                                  |     `OrderbookSnapshotEvent`     |
                                  +-----------------+------------------+
                                                    |
                                                    v [Evaluates Snapshot]
                                  +-----------------+------------------+
                                  |        `SpoofingDetector`        |
                                  +--------+------------------+--------+
                                           |                  |
                       Heuristic Engine    v                  v   ML Engine (LSTM Autoencoder)
                     [Imbalance Threshold]              [Temporal Reconstruction Anomaly]
                                           \                  /
                                            \                /
                                             v              v
                                  +-----------------+------------------+
                                  |       `SPOOFING_SUSPICION`         |
                                  |             (Alert)                |
                                  +------------------------------------+
```

### Ingestion Details
1. **Source Topic**: The upstream order matching engine publishes high-density snapshot events to the `nte.orderbook.snapshots` Kafka topic.
2. **Schema Ingestion**: These snapshots are processed and validated against the [[entities/orderbook-snapshot-event]] Pydantic schema, ensuring strict structure and typing of bid/ask depths (Levels 1 to 5) and metadata.
3. **Execution Routing**: The validated events are evaluated by the `SpoofingDetector` in batches or real-time micro-batches.

---

## 2. Core Detection Methodologies

The `SpoofingDetector` leverages a dual-layer approach: a **High-Velocity Heuristic Matcher** for deterministic, low-latency alerting, and an **LSTM Autoencoder** to capture latent, multi-dimensional anomalies in the orderbook's time-series evolution.

### 2.1 The Heuristic Layer: Orderbook Imbalance (OBI) & Cancellation Velocity
The primary indicator of a spoofing attempt is a massive structural tilt in the orderbook's volume profile, accompanied by a spike in order cancellations.

The metric used to track this tilt is the **L1-L5 Orderbook Imbalance (OBI)**, formulated as:

$$\text{Imbalance} = \frac{\sum_{i=1}^{5} \text{bid\_v}_i - \sum_{i=1}^{5} \text{ask\_v}_i}{\sum_{i=1}^{5} \text{bid\_v}_i + \sum_{i=1}^{5} \text{ask\_v}_i + \epsilon}$$

Where $\epsilon = 10^{-9}$ is a stabilization constant preventing division-by-zero errors. For a deeper mathematical breakdown of this formulation and its boundary states, see [[concepts/orderbook-imbalance-formula]].

#### Detection Rule:
An alert is triggered when both of the following conditions are met:
1. **Extreme Imbalance**: The absolute value of the imbalance metric exceeds a configured threshold ($I_{th}$), typically set to `0.8` (representing an $80\%$ volume bias to one side of the book).
2. **High Cancellation Velocity**: The modern cancellation rate (`cancel_rate` over the last second) exceeds a threshold ($C_{th}$), typically set to `0.9` ($90\%$ of total order events in that window were cancellations).

```python
# Conceptual execution within SpoofingDetector
imbalance = (total_bid - total_ask) / (total_bid + total_ask + 1e-9)

if abs(imbalance) > self.imbalance_threshold and row['cancel_rate'] > self.cancel_rate_threshold:
    # Trigger alert
```

### 2.2 The Production ML Layer: LSTM Autoencoder
While the heuristic layer captures blunt force spoofing, sophisticated spoofing routines distribute orders across multiple price levels to evade simple limits. To combat this, the production-tier detector integrates an **LSTM Autoencoder** trained on normal market liquidity patterns.

* **Features**: A sliding time-series window ($T$) containing the following vector:
  $$\mathbf{x}_t = \left[ \text{bid\_v}_{1 \dots 5}, \text{ask\_v}_{1 \dots 5}, \text{cancel\_rate} \right]$$
* **Mechanism**: The Autoencoder compresses the sequence into a low-dimensional bottleneck representation and then reconstructs it.
* **Inference Alerting**: If the reconstruction error (Mean Squared Error between the input and the reconstructed window) exceeds a dynamically calculated historical threshold, it indicates a highly non-bona-fide liquidity injection pattern, triggering an alert.

---

## 3. Implementation Details

The core processing logic resides in `src/models/spoofing_detector.py`:

```python
import numpy as np
import pandas as pd
import logging

logger = logging.getLogger(__name__)

class SpoofingDetector:
    def __init__(self, imbalance_threshold=0.8, cancel_rate_threshold=0.9):
        self.imbalance_threshold = imbalance_threshold
        self.cancel_rate_threshold = cancel_rate_threshold
        logger.info(f"Initialized SpoofingDetector with imbalance_threshold={imbalance_threshold}")

    def evaluate_snapshot(self, snapshot_df: pd.DataFrame):
        """
        Evaluates nte.orderbook.snapshots for spoofing patterns.
        """
        alerts = []
        
        # Calculate Orderbook Imbalance (L1-L5)
        for _, row in snapshot_df.iterrows():
            total_bid = sum([row[f'bid_v_{i}'] for i in range(1, 6)])
            total_ask = sum([row[f'ask_v_{i}'] for i in range(1, 6)])
            
            imbalance = (total_bid - total_ask) / (total_bid + total_ask + 1e-9)
            
            # Heuristic + Ensemble ML prediction
            if abs(imbalance) > self.imbalance_threshold and row['cancel_rate'] > self.cancel_rate_threshold:
                alerts.append({
                    'type': 'SPOOFING_SUSPICION',
                    'instrument': row['instrument'],
                    'timestamp': row['timestamp'],
                    'imbalance': imbalance,
                    'cancel_rate': row['cancel_rate'],
                    'severity': 'HIGH'
                })
                
        return alerts
```

---

## 4. Architectural Principles

### Decoupling Analytics from Transport
In accordance with [[decisions/decoupling-analytics-transport|Decoupling Analytics from Transport]], the `SpoofingDetector` is entirely decoupled from the Kafka ingestion loops and Spark infrastructure. It accepts standardized Python primitives (such as Pandas DataFrames or Pydantic models) so that model evaluation can be unit-tested offline without spin-up overhead.

### Decoupled Alerts & Downstream Persistence
Once a spoofing anomaly is captured:
1. The alert is formatted into a standardized `SPOOFING_SUSPICION` event.
2. It is dispatched immediately back to the Kafka cluster on the `gfmg.compliance.alerts` topic.
3. According to [[decisions/decoupled-persistence|Decoupled Persistence Design]], the live storage engine remains stateless; long-term analytical histories, model training logs, and audit trails are persisted directly into Google BigQuery `gfmg_surveillance.alerts_log` for offline compliance forensics and ML retraining loops.

---

## See Also
* [[concepts/orderbook-imbalance-formula]] — In-depth mathematical formulation and edge cases of the imbalance calculation.
* [[entities/orderbook-snapshot-event]] — The underlying data model used to represent the book profiles evaluated by this system.
* [[concepts/surveillance-stream-processor]] — The high-throughput framework hosting this model's execution loop.
* [[decisions/decoupling-analytics-transport]] — Design justification for abstracting model logic away from message queues.