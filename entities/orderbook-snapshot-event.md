<!-- anchor: src/models/spoofing_detector.py:L1-L100 sha:HEAD -->

# Orderbook Snapshot Event Model

The `OrderbookSnapshotEvent` is a strongly-typed Pydantic data model that defines the schema for Level 1 through Level 5 (L1–L5) orderbook depth profiles. Emitted by the core matching engine onto the `nte.orderbook.snapshots` Kafka topic, this model serves as the structural validation layer for the real-time stream processing pipeline, enabling downstream components to evaluate market microstructure anomalies.

---

## Responsibilities

* **Schema Enforcement**: Guarantees type safety and structural compliance for incoming market-depth event payloads consumed by the [[concepts/surveillance-stream-processor]].
* **Microstructure Representation**: Captures localized market liquidity states up to five depth tiers (L1–L5 bid and ask volumes) alongside historical order cancellation velocity.
* **Feature Provisioning**: Exposes standardized numeric fields necessary for calculating orderbook pressure in the [[concepts/orderbook-imbalance-formula]] and running inference on the [[concepts/spoofing-detection]] subsystem.

---

## Dependencies

* **Pydantic**: Uses `pydantic.BaseModel` and `pydantic.Field` to perform strict, runtime parsing and validation of JSON payloads.
* **Upstream Matching Engine**: Relies on the core Order Matching Engine to emit accurate, synchronized snapshot records.
* **Downstream Consumers**: 
  * [[concepts/surveillance-stream-processor]]: Acts as the consumer loop driver that ingests and deserializes these events.
  * [[concepts/spoofing-detection]]: Uses validated snapshot instances to construct localized feature matrices for spoofing heuristic evaluations and deep-learning (LSTM Autoencoder) inference.

---

## Pydantic Model Implementation

The model is defined within the system's shared schema directory (`src/consumers/schemas.py`):

```python
from pydantic import BaseModel, Field

class OrderbookSnapshotEvent(BaseModel):
    instrument: str = Field(..., description="Symbol of the instrument")
    timestamp: int = Field(..., description="Epoch timestamp of snapshot")
    cancel_rate: float = Field(..., description="Rate of cancelled orders in the last second")
    bid_v_1: float = Field(..., description="Bid volume at Level 1 (best bid)")
    bid_v_2: float = Field(..., description="Bid volume at Level 2")
    bid_v_3: float = Field(..., description="Bid volume at Level 3")
    bid_v_4: float = Field(..., description="Bid volume at Level 4")
    bid_v_5: float = Field(..., description="Bid volume at Level 5")
    ask_v_1: float = Field(..., description="Ask volume at Level 1 (best ask)")
    ask_v_2: float = Field(..., description="Ask volume at Level 2")
    ask_v_3: float = Field(..., description="Ask volume at Level 3")
    ask_v_4: float = Field(..., description="Ask volume at Level 4")
    ask_v_5: float = Field(..., description="Ask volume at Level 5")
```

---

## Field Specifications

| Field Name | Type | Description | Analytical Relevance |
| :--- | :--- | :--- | :--- |
| `instrument` | `str` | Financial instrument symbol (e.g., `TSLA`, `AAPL`). | Groups snapshots and routes calculations per asset. |
| `timestamp` | `int` | Unix epoch timestamp of the snapshot. | Provides temporal alignment for time-series windowing. |
| `cancel_rate` | `float` | Rate of cancelled orders within the preceding 1-second window. | Combined with book imbalance to identify phantom liquidity. |
| `bid_v_1` to `bid_v_5` | `float` | Cumulative buy order volume across tiers 1 through 5. | Formulates the aggregate demand profile of the orderbook. |
| `ask_v_1` to `ask_v_5` | `float` | Cumulative sell order volume across tiers 1 through 5. | Formulates the aggregate supply profile of the orderbook. |

---

## Downstream Analytical Processing

Once validated, the data captured by `OrderbookSnapshotEvent` is translated into a structured format (such as a Pandas DataFrame) to support calculations within the [[concepts/spoofing-detection]] pipeline. 

Specifically, the L1–L5 volume metrics are utilized to calculate the instantaneous orderbook imbalance using the [[concepts/orderbook-imbalance-formula]]:

$$\text{Imbalance} = \frac{\sum_{i=1}^{5} \text{bid\_v}_i - \sum_{i=1}^{5} \text{ask\_v}_i}{\sum_{i=1}^{5} \text{bid\_v}_i + \sum_{i=1}^{5} \text{ask\_v}_i + \epsilon}$$

Where $\epsilon = 10^{-9}$ is a safety constant to prevent division-by-zero exceptions. 

### Separation of Concerns
In compliance with [[decisions/decoupling-analytics-transport]], this Pydantic representation remains fully independent of physical transport layers. This isolation allows the core detection algorithms to ingest mock `OrderbookSnapshotEvent` instances directly inside offline testing environments, removing any runtime reliance on a live Apache Kafka or PySpark streaming cluster.

## Related Models
* [[entities/trade-matched-event]]: Models transaction matching records, used primarily by the [[concepts/wash-trade-detection]] subsystem.
* [[entities/settlement-status-event]]: Tracks post-trade clearing states for compliance verification.