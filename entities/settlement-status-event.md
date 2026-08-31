<!-- anchor: docs/architecture.md:L1-L100 sha:HEAD -->

# Settlement Status Event

The `SettlementStatusEvent` is a strongly-typed Pydantic data model that represents post-trade clearance states emitted by the **SecureClear Financial Services (SCFS)** trade settlement engine. It is ingested via the `scfs.settlement.status` Kafka topic and serves as a foundational data stream for post-trade compliance validation and Anti-Money Laundering (AML) monitoring.

This model is a core component of the ingestion layer within [[summaries/project-astrophage]] and is validated at runtime before downstream consumption by the [[concepts/rules-engine-aml]].

---

## Data Model Definition

Defined in `src/consumers/schemas.py`, the `SettlementStatusEvent` uses Pydantic to enforce strict data types, ensuring payload validation at the stream boundary.

```python
from pydantic import BaseModel, Field

class SettlementStatusEvent(BaseModel):
    trade_id: str = Field(
        ..., 
        description="Unique identifier correlating back to the matched execution"
    )
    status: str = Field(
        ..., 
        description="Current clearance state of the trade. Must be one of: PENDING, SETTLED, FAILED"
    )
    settlement_time: int = Field(
        ..., 
        description="Epoch millisecond timestamp indicating when the status state was committed"
    )
```

### Schema Field Details

| Field | Type | Description | Compliance Significance |
| :--- | :--- | :--- | :--- |
| `trade_id` | `str` | Reference ID correlating directly to a [[entities/trade-matched-event]]. | Allows the pipeline to join execution properties (price, volume, parties) with settlement outcomes. |
| `status` | `str` | The physical state of clearance: `PENDING`, `SETTLED`, `FAILED`. | Used to filter out unexecuted or failed trades from final AML structuring thresholds. |
| `settlement_time` | `int` | Unix Epoch timestamp (milliseconds) of the settlement state transition. | Determines window boundaries for sliding-window velocity checks (e.g., 24-hour structuring windows). |

---

## Responsibilities

The `SettlementStatusEvent` model is tasked with several crucial operational and analytical roles:

* **Ingestion Integrity**: Acts as the strict schema validator for all incoming messages on the `scfs.settlement.status` Kafka topic managed by the [[concepts/surveillance-stream-processor]].
* **Post-Trade Audit Trail Correlation**: Correlates the clearance status of financial assets back to their trade execution origin (`[[entities/trade-matched-event]]`), allowing compliance analysts to verify that matched orders successfully cleared.
* **AML Signal Generation**: Feeds the [[concepts/rules-engine-aml]], validating cash flow state transitions to identify structuring maneuvers (e.g., rapid, consecutive sub-threshold settlements).
* **Decoupled Architecture Verification**: Ensures structural compatibility when running offline simulation tests without active Kafka instances, honoring the system's core design patterns in [[decisions/decoupling-analytics-transport]].

---

## Dependencies

The `SettlementStatusEvent` operates within a highly decoupled, reactive network of services:

* **Upstream Producer**: 
  * Emitted by the **SecureClear Financial Services (SCFS)** Trade Settlement System when a trade changes its clearance state.
* **In-Memory Ingestion Layer**: 
  * Parsed and instantiated by the [[concepts/surveillance-stream-processor]] driver loop (`SurveillanceStreamProcessor`).
* **Downstream Consumers**:
  * **[[concepts/rules-engine-aml]]**: Consumes verified events to evaluate rules defined in [[entities/aml-config]], specifically targeting structured patterns such as rule `AML_001` (High Velocity Structuring).
* **Downstream Persistence**:
  * Upon rule evaluation and potential alert generation, downstream systems write results to `gfmg.compliance.alerts` and the BigQuery `gfmg_surveillance.alerts_log` according to the [[decisions/decoupled-persistence]] pattern.

---

## Operational Workflow in AML Pipeline

```
     SCFS Settlement Engine
               |
               v (scfs.settlement.status)
 +-------------------------------------------+
 | [[concepts/surveillance-stream-processor]] | (Validates schema via Pydantic)
 +-------------------------------------------+
               | 
               | Instantiates SettlementStatusEvent
               v
     [[concepts/rules-engine-aml]]
               |
               +---> Reads Rules from [[entities/aml-config]] (e.g., AML_001)
               |
               +---> Queries State to correlate with [[entities/trade-matched-event]]
               |
               v (If threshold violated)
    Trigger Compliance Alert -> Publish to BigQuery ([[decisions/decoupled-persistence]])
```

### High-Velocity Structuring Evaluation (AML_001)

The Pydantic model is key to evaluating structuring rules defined in the declarative schema of [[entities/aml-config]]. For example, rule `AML_001` requires tracing the total dollar value of settled trades within a moving 24-hour window:

$$\text{Total Settled USD} = \sum_{t \in T_{\text{settled}}} \text{Price}_t \times \text{Quantity}_t$$

By utilizing the `trade_id` from the `SettlementStatusEvent`, the platform resolves the underlying instrument, execution price, and volume from the corresponding [[entities/trade-matched-event]], ignores `FAILED` trades, and flags any account aggregates that approach regulatory reporting ceilings (e.g., \$9,999.00 USD) over the time window specified by the settlement timestamps.