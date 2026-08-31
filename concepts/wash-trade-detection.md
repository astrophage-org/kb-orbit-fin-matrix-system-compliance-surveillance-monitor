# Wash Trade Detection via Graph Cycle Analysis

In financial markets, wash trading is a form of market manipulation where market participants collude or act independently to execute offsetting trades. This activity creates a false or misleading impression of active trading volume and price discovery for a specific financial instrument. Within the **Compliance Surveillance Monitor (Project Astrophage)**, the `WashTradeDetector` operates as a graph-based processing block that analyzes trade execution sequences in real time to identify circular transaction networks (e.g., $A \to B \to C \to A$).

The implementation relies on directed graph construction and cycle detection algorithms, decoupled from transport concerns to support ultra-low-latency real-time streaming as well as offline, deterministic replay verification.

---

## 1. Subsystem Architecture & Stream Integration

The `WashTradeDetector` sits downstream from the [[concepts/surveillance-stream-processor]], consuming validated transaction events.

```
+--------------------------------------------------+
|           nte.trades.matched (Kafka)             |
+------------------------+-------------------------+
                         |
                         | (Consumes raw payloads)
                         v
+--------------------------------------------------+
|      [[concepts/surveillance-stream-processor]]      |
|    - Normalizes raw streams using Pydantic       |
+------------------------+-------------------------+
                         |
                         | Maps to [[entities/trade-matched-event]]
                         v
+--------------------------------------------------+
|            [[concepts/wash-trade-detection]]     |
|   - Constructs Directed Graph (DiGraph)          |
|   - Executes Depth-First Search for cycles       |
+------------------------+-------------------------+
                         |
                         | Emits alert payload on cycle detection
                         v
+--------------------------------------------------+
|             Downstream Dispatches                |
| - Kafka: gfmg.compliance.alerts                  |
| - Warehouse: BigQuery alerts_log                 |
+--------------------------------------------------+
```

As detailed in [[decisions/decoupling-analytics-transport]], the analytical logic is kept completely stateless and database-agnostic. The detection algorithms ingest standard `pandas.DataFrame` representation of [[entities/trade-matched-event]] records. This isolation ensures that the complex traversal logic can be thoroughly verified through unit tests without requiring a live PySpark environment, active Kafka clusters, or external databases.

---

## 2. Mathematical & Graph Formulation

The wash trade detection engine represents the trading network as a dynamic, directed multigraph $G = (V, E)$ defined over a localized sliding time window $\Delta t$:

$$G = (V, E)$$

Where:
* **Vertices ($V$)**: The set of unique market participant identifiers (traders/clearing accounts) present in the current batch.
  $$V = \{ \text{trader\_id}_i \}$$
* **Directed Edges ($E$)**: An edge $e = (u, v)$ represents a validated trade where participant $u$ is the **seller** and participant $v$ is the **buyer**.
  $$e = (\text{seller\_id}, \text{buyer\_id})$$
* **Edge Weights ($W$)**: Each directed edge carries metadata, with the edge weight representing the cumulative transaction volume (quantity) exchanged from seller to buyer for a given instrument during the sliding window.
  $$W(u, v) = \sum_{i} \text{quantity}_i$$

### Cycle Detection Logic
A wash trade loop is defined as a directed cycle $C$ within $G$:

$$C = (v_1, v_2, \dots, v_k, v_1)$$

where:

$$(v_i, v_{i+1}) \in E \quad \text{for} \quad 1 \le i < k, \quad \text{and} \quad (v_k, v_1) \in E$$

The system triggers a warning alert when a directed cycle is closed. The edge weights and topological structure determine the confidence of the match. For instance, highly symmetrical volumes across the cycle denote a structural wash loop ($A \to B \to A$ with identical share amounts), which is given a static confidence metric of `0.95` in standard configurations.

---

## 3. Algorithmic Implementation

The detection engine uses the `NetworkX` library to construct directed graphs dynamically and traverse them.

Below is the implementation found in `src/models/wash_trade_detector.py`:

```python
import pandas as pd
import networkx as nx
import logging

logger = logging.getLogger(__name__)

class WashTradeDetector:
    def __init__(self, time_window_ms=5000):
        self.time_window_ms = time_window_ms
        self.trade_graph = nx.DiGraph()
        
    def process_batch(self, trades_df: pd.DataFrame):
        """
        Process a DataFrame of matched trades from nte.trades.matched
        """
        alerts = []
        for _, row in trades_df.iterrows():
            buyer = row['buyer_id']
            seller = row['seller_id']
            instrument = row['instrument']
            
            # Add edge
            if self.trade_graph.has_edge(seller, buyer):
                self.trade_graph[seller][buyer]['weight'] += row['quantity']
            else:
                self.trade_graph.add_edge(seller, buyer, weight=row['quantity'], instrument=instrument)
                
            # Check for cycles (A -> B -> C -> A)
            try:
                cycles = nx.find_cycle(self.trade_graph, source=seller, orientation='original')
                if cycles:
                    alerts.append({
                        'type': 'WASH_TRADE_CYCLE',
                        'entities': [u for u, v, _ in cycles],
                        'instrument': instrument,
                        'confidence': 0.95
                    })
                    self.trade_graph.clear() # Reset after detection for simplicity
            except nx.NetworkXNoCycle:
                pass
                
        return alerts
```

### Depth-First Search (DFS) Traversal
The cycle search relies on a modified Depth-First Search (DFS) optimized by `NetworkX` (`nx.find_cycle`). The search is rooted at the `seller` node of the newly inserted trade execution edge. Because cycle evaluation occurs incrementally upon edge insertion, the search complexity is bounded by $O(V + E)$ within the localized sliding window.

---

## 4. State Management and Memory Cleansing

Because the underlying streaming pipeline is continuous and handles high-frequency trade matches, managing the growth of $G$ is critical to preventing out-of-memory (OOM) failures.

### Stateless Graph Cleansing
To prevent memory exhaustion and eliminate duplicate alert cascades (where the same cycle triggers alerts continuously on subsequent batches), the engine invokes an immediate graph flush upon positive identification:

```python
self.trade_graph.clear()
```

While simple, this immediate clearing prevents duplicate alerts on overlapping cycles. However, in production deployment configurations, a more granular time-based edge-pruning mechanism is utilized to remove stale edges exceeding `time_window_ms` without clearing the entire graph topology. This state pruning and buffer optimization is covered in detail in the [[concepts/stateless-graph-cleansing]] documentation.

---

## 5. Interaction with Rules and Detection Pipelines

The `WashTradeDetector` is part of a multi-tiered surveillance suite:
1. **Microstructure Level**: Parallel with this graph-cycle traversal, the [[concepts/spoofing-detection]] engine runs time-series evaluations on orderbook snapshots to detect market manipulation tactics near the top of the book using the [[concepts/orderbook-imbalance-formula]].
2. **Post-Trade Sanctions & Compliance**: Concurrently, the [[concepts/rules-engine-aml]] acts on downstream post-trade signals (such as [[entities/settlement-status-event]]) to evaluate regulatory rules (like structuring thresholds) defined in the system's declarative configuration schema ([[entities/aml-config]]).

### Alert Propagation & Storage
Once a cycle alert is constructed:
1. It is published back to the `gfmg.compliance.alerts` Kafka stream.
2. It is consumed asynchronously and stored permanently in Google BigQuery under `gfmg_surveillance.alerts_log` according to the architecture's [[decisions/decoupled-persistence]] strategy.

---

## 6. Execution Trace Example

Consider the following stream of transaction events represented by [[entities/trade-matched-event]] structures:

| Trade ID | Buyer ID | Seller ID | Instrument | Quantity | Timestamp |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `T-01` | `TraderB` | `TraderA` | `AAPL` | 100 | 1630000001 |
| `T-02` | `TraderC` | `TraderB` | `AAPL` | 100 | 1630000002 |
| `T-03` | `TraderA` | `TraderC` | `AAPL` | 100 | 1630000003 |

### Execution Phase Analysis

```
Step 1: Trade T-01 Processed
    Graph: (TraderA) --[qty=100]--> (TraderB)
    Cycle Check: No cycle detected.

Step 2: Trade T-02 Processed
    Graph: (TraderA) --[qty=100]--> (TraderB) --[qty=100]--> (TraderC)
    Cycle Check: No cycle detected.

Step 3: Trade T-03 Processed
    Graph: (TraderA) --[qty=100]--> (TraderB) --[qty=100]--> (TraderC) --[qty=100]--> (TraderA)
    Cycle Check: Cycle identified! 
                 Sequence: [TraderA, TraderB, TraderC]
                 Alert Emitted: WASH_TRADE_CYCLE
                 Graph Action: self.trade_graph.clear() invoked.
```

By immediately purging the graph upon cycle detection, the monitor ensures that the system is ready to evaluate subsequent events cleanly, keeping the memory footprint minimal and eliminating circular search overhead.