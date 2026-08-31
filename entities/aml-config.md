<!-- anchor: docs/architecture.md:L1-L100 sha:HEAD -->

# AML Configuration Schema

The `aml_config.json` configuration file serves as the single source of truth for declarative regulatory thresholds, watchlist-matching mandates, and target Kafka topics within the **Compliance Surveillance Monitor** (Project Astrophage). By formalizing these rules in a structured JSON layout, the platform decouples business-level compliance policies from core execution logic, matching the architectural decisions outlined in [[decisions/decoupling-analytics-transport]].

---

## Responsibilities

* **Declarative Rule Definition**: Isolates compliance parameters (e.g., window intervals, transaction limits, thresholds) from stream processing source code.
* **Stream Routing Manifest**: Declares which Kafka topics (`topics_required`) must be ingested and channeled to the [[concepts/rules-engine-aml]] for evaluation.
* **Hot-Reload Support**: Serves as a validated payload structure that can be updated dynamically at runtime without requiring recompilation or redeployment of [[concepts/surveillance-stream-processor]].
* **Standardized Metadata**: Emits structural validation context (such as versioning, tracking identifiers, and corporate ownership) for downstream auditing in BigQuery as per [[decisions/decoupled-persistence]].

---

## Dependencies

* **Consumers & Engine**: 
  * Loaded at bootstrap by `src/main.py` to count and log active surveillance rulesets.
  * Parsed and evaluated by the [[concepts/rules-engine-aml]] to match streaming transactional fields.
* **Data Models & Schemas**:
  * Targets [[entities/settlement-status-event]] (via `scfs.settlement.status`) for transactional structuring checks (`AML_001`).
  * Targets [[entities/trade-matched-event]] (via `nte.trades.matched`) for party verification checks (`AML_002`).
* **High-Level Systems**:
  * Governed by [[summaries/project-astrophage]] context for real-time compliance alerting.

---

## Schema Structure

The active configuration payload located in `rules/aml_config.json` conforms to the structural schema outlined below:

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "ComplianceSurveillanceAMLConfig",
  "type": "object",
  "required": ["version", "last_updated", "organization", "rules"],
  "properties": {
    "version": {
      "type": "string",
      "description": "Semantic versioning identifier for tracking updates to regulatory parameters."
    },
    "last_updated": {
      "type": "string",
      "format": "date-time",
      "description": "ISO-8601 timestamp representing the last modification time of the rules file."
    },
    "organization": {
      "type": "string",
      "description": "The legal entity or project partition implementing these compliance regulations."
    },
    "rules": {
      "type": "array",
      "description": "An array of independent analytical validation structures applied against stream data.",
      "items": {
        "type": "object",
        "required": ["id", "name", "description", "topics_required"],
        "properties": {
          "id": {
            "type": "string",
            "pattern": "^AML_[0-9]{3}$",
            "description": "Unique rule identifier mapped to downstream alert taxonomies."
          },
          "name": {
            "type": "string",
            "description": "Human-readable label for dashboard and log reporting."
          },
          "description": {
            "type": "string",
            "description": "Clear technical and regulatory explanation of what this rule catches."
          },
          "thresholds": {
            "type": "object",
            "description": "Numerical limits, counts, and window spans used by the rules evaluation processor.",
            "properties": {
              "max_amount_usd": {
                "type": "number",
                "description": "The maximum currency value allowed before flagging or aggregate counting starts."
              },
              "count_limit": {
                "type": "integer",
                "description": "Number of operations allowed within the time window before generating a violation."
              },
              "time_window_hours": {
                "type": "integer",
                "description": "Moving temporal boundary represented in hours."
              }
            },
            "additionalProperties": false
          },
          "topics_required": {
            "type": "array",
            "description": "Specific Kafka topics that need to be processed to execute this rule validation.",
            "items": {
              "type": "string"
            }
          }
        }
      }
    }
  }
}
```

---

## Active Configuration Instances

### AML_001: High Velocity Structuring
* **Goal**: Identify attempts to circumvent the \$10,000 regulatory reporting threshold by breaking down transactions into small, rapid iterations.
* **Trigger Mechanics**: If an entity generates more than `count_limit` transactions below `max_amount_usd` within a `time_window_hours` sliding cycle, an alert is dispatched.
* **Event Target**: [[entities/settlement-status-event]] payloads routed via the `scfs.settlement.status` Kafka stream.

### AML_002: Sanctioned Entity Interaction
* **Goal**: Instant validation of trade counterparts against global watchlists (e.g., OFAC).
* **Trigger Mechanics**: Scans incoming buyers and sellers against cached sanction list indices.
* **Event Target**: [[entities/trade-matched-event]] payloads routed via the `nte.trades.matched` Kafka stream.

```json
{
    "version": "1.0",
    "last_updated": "2026-08-17T00:00:00Z",
    "organization": "Astrophage",
    "rules": [
        {
            "id": "AML_001",
            "name": "High Velocity Structuring",
            "description": "Detects multiple sub-threshold transactions within a 24-hour period.",
            "thresholds": {
                "max_amount_usd": 9999.00,
                "count_limit": 5,
                "time_window_hours": 24
            },
            "topics_required": ["scfs.settlement.status"]
        },
        {
            "id": "AML_002",
            "name": "Sanctioned Entity Interaction",
            "description": "Cross-references buyer_id and seller_id with OFAC lists.",
            "topics_required": ["nte.trades.matched"]
        }
    ]
}
```

---

## Processing Flow & Rule Parsing

The configuration is parsed sequentially at bootstrap. Inside `src/main.py`, the dynamic engine reads this JSON, parses the array of rules, and maps target topics:

```
                  [ aml_config.json File ]
                             │
                             ▼
                    [ main.py Startup ]
                             │
            ┌────────────────┴────────────────┐
            ▼                                 ▼
   [ Stream Setup ]                  [ Log Active Rules ]
   Extract topics_required           Register rule parameters 
   to subscribe Kafka.               with [[concepts/rules-engine-aml]].
```

This structural architecture ensures that modifications to thresholds (such as reducing `max_amount_usd` to \$4,999.00 for heightened auditing periods) only require updating the raw config file rather than modifying Java/Scala/Python source files.