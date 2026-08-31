# Stateless Graph Cleansing

In high-throughput trading systems, tracking relationship structures across streaming transactions is a major challenge for stateful memory management. Within the **Compliance Surveillance Monitor (Project Astrophage)**, the `[[concepts/wash-trade-detection]]` system builds dynamic transaction graphs using NetworkX to identify circular trading networks (e.g., $A \to B \to C \to A$). 

Without a proactive memory management strategy, these graphs would grow unboundedly, resulting in memory exhaustion (Out Of Memory errors) and exponential CPU degradation during cycle detection. **Stateless Graph Cleansing** is the architectural pattern used to manage this cycle buffer, ensuring low-latency execution and constant-bound memory footprints.

---

## 1. The Memory & Computational Challenge

The system consumes trade events via the `[[concepts/surveillance-stream-processor]]` parsed as `[[entities/trade-matched-event]]` instances. These events are processed in micro-batches.

```
[ Trade Event Stream ] ---> ( WashTradeDetector ) ---> [ NetworkX DiGraph (Stateful Memory) ]
                                                            |
                                                            +---> Graph Growth: O(V + E)
                                                            +---> Cycle Detection: O(V + E)
```

As transactions stream in:
1. **Node and Edge Growth ($O(V + E)$):** Every new trader adds a vertex ($V$), and every trade matching pair adds a directed edge ($E$).
2. **Cycle Detection Latency Degradation:** NetworkX uses Depth-First Search (DFS) algorithms under the hood for `nx.find_cycle`. The computation time for finding cycles scales linearly with graph size $O(V + E)$. In highly active markets, an unpruned graph becomes a performance bottleneck within minutes.
3. **Alert Cascades:** If a cycle exists (e.g., $A \to B \to A$) and is not cleared, subsequent trades between these nodes will continue to trigger duplicate alerts, flooding the downstream ingestion topics and polluting `[[decisions/decoupled-persistence]]` layers like BigQuery.

---

## 2. Graph Cleansing & Buffer Mitigation Strategies

To handle these challenges, `WashTradeDetector` implements an aggressive memory-cleansing heuristic.

```
       [ Add Edge (Seller -> Buyer) ]
                    |
                    v
         [ nx.find_cycle() ]
          /               \
   (Cycle Found)     (No Cycle)
        /                   \
       v                     v
[ Append Alert ]       [ Keep Edge ]
       |                     |
[ self.trade_graph.clear() ]  v
       |               [ Await Next Edge ]
       v
  (Reset State)
```

### 2.1 Immediate Post-Detection Reset
The primary mechanism utilized in `src/models/wash_trade_detector.py` is the **Post-Detection Reset**:

```python
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
```

By calling `self.trade_graph.clear()`, the underlying dictionary structure of the `nx.DiGraph` is completely emptied. 

#### Advantages
* **Memory Ceiling:** The memory footprint is guaranteed to drop to $O(1)$ instantly after a cycle detection event.
* **Alert De-duplication:** Prevents redundant alerts on the same circular subgraph path. Once a wash cycle is flagged, the graph starts fresh.
* **Garbage Collection:** Facilitates immediate garbage collection of orphaned node strings and edge attributes.

---

## 3. Comparative Architecture: Sliding Time-Window Pruning

While an immediate reset is highly performant and simple to implement, it is a destructive operation that clears unrelated, benign trading edges. An alternative, more granular approach involves **Sliding Window Pruning** using the `time_window_ms` parameter defined in the constructor:

$$\text{Active Edge Set } E_{active} = \{ e \in E \mid T_{current} - T_{edge} \le \Delta t \}$$

In a sliding-window configuration, each edge carries an associated timestamp metadata attribute. A pruning loop periodically runs to clear stale edges:

```python
def prune_stale_edges(self, current_timestamp_ms: int):
    """
    Alternative granular cleanup strategy: Removes edges older than the window threshold.
    """
    stale_edges = []
    for u, v, data in self.trade_graph.edges(data=True):
        if current_timestamp_ms - data['timestamp'] > self.time_window_ms:
            stale_edges.append((u, v))
            
    self.trade_graph.remove_edges_from(stale_edges)
    
    # Remove isolated nodes (nodes with degree 0) to prevent node memory leaks
    isolated_nodes = list(nx.isolates(self.trade_graph))
    self.trade_graph.remove_nodes_from(isolated_nodes)
```

### Trade-off Analysis

| Metric / Feature | Immediate Reset (`.clear()`) | Sliding Window Pruning (Timestamp TTL) |
| :--- | :--- | :--- |
| **Computational Overhead** | $O(1)$ amortized. Highly efficient. | $O(E)$ to scan and prune edges periodically. |
| **Memory Guarantee** | Strict reset to baseline. | Variable; dependent on volume within the window. |
| **Detection Recall** | Lower (clears partial graphs, potentially missing overlapping cycles). | Higher (maintains concurrent, non-overlapping paths). |
| **Implementation Complexity**| Minimal. No timestamp comparisons or isolate sweeping required. | Moderate. Requires sweeping isolated nodes and maintaining index structures. |

Because `[[summaries/project-astrophage]]` operates under extreme low-latency requirements, the **Immediate Reset** strategy is preferred as it optimizes CPU cycle availability for the main streaming thread.

---

## 4. Interaction with Stream Processing & Testing

This stateless design aligns directly with the architectural pattern described in `[[decisions/decoupling-analytics-transport]]`. Because the model maintains a self-cleansing, transient graph structure, it requires no persistent database lookups or disk writes to execute its evaluation pipeline. 

During test execution, deterministic batches of `[[entities/trade-matched-event]]` can be passed to the detector. The rapid cycles of `clear()` execution ensure that tests do not leak state across assertions, enabling clean, reproducible unit and integration test suites.