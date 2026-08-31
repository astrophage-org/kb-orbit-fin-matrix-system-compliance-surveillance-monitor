# Orderbook Imbalance Formulation (L1-L5 Depth)

The tracking of orderbook imbalance across multiple depth levels is a cornerstone metric for identifying microstructural anomalies, specifically market spoofing. The Project Astrophage platform implements a multi-level L1–L5 depth imbalance calculation within the `[[concepts/spoofing-detection]]` subsystem to reveal rapid shifts in buy and sell pressure.

This page outlines the mathematical foundations of the L1-L5 imbalance formula, its physical interpretation, and its role in downstream anomaly detection pipelines.

---

## 1. Mathematical Formulation

The orderbook imbalance metric evaluates the normalized difference between cumulative bid volume and cumulative ask volume up to Level 5 (L5) depth. 

$$\text{Imbalance} (I) = \frac{\sum_{i=1}^{5} V_{b,i} - \sum_{i=1}^{5} V_{a,i}}{\sum_{i=1}^{5} V_{b,i} + \sum_{i=1}^{5} V_{a,i} + \epsilon}$$

Where:
* $V_{b,i}$ represents the **bid volume** at depth level $i$ (where $i=1$ is top-of-book/best bid, and $i=5$ is the fifth price level down).
* $V_{a,i}$ represents the **ask volume** at depth level $i$ (where $i=1$ is top-of-book/best ask, and $i=5$ is the fifth price level up).
* $\epsilon = 10^{-9}$ is a regularization constant (infinitesimal float) introduced to prevent division-by-zero exceptions in illiquid or temporarily cleared orderbooks.

### Properties of the Imbalance Metric ($I$)

The imbalance metric is bounded as a continuous variable within the interval $[-1.0, 1.0]$:

| Value Range | Interpretative State | Market Pressure Indicator |
| :--- | :--- | :--- |
| $I \to 1.0$ | **Extreme Bid Dominance** | Massive buying interest or non-bona-fide bid layering (spoofing to buy). |
| $I \to 0.0$ | **Symmetric Book** | Balanced liquidity distribution; normal supply and demand. |
| $I \to -1.0$ | **Extreme Ask Dominance** | Massive selling interest or non-bona-fide ask layering (spoofing to sell). |

---

## 2. Ingestion Context and Data Modeling

The raw inputs for this calculation are consumed from the `nte.orderbook.snapshots` topic by the `[[concepts/surveillance-stream-processor]]`.

```
        [ Kafka: nte.orderbook.snapshots ]
                        |
                        v
          Parsed & Validated Payload
        [[entities/orderbook-snapshot-event]]
                        |
                        v
             [ SpoofingDetector ]
          (L1-L5 Imbalance Calculation)
```

The fields mapping directly to this calculation are defined inside the `[[entities/orderbook-snapshot-event]]` schema:

* Bids: `bid_v_1`, `bid_v_2`, `bid_v_3`, `bid_v_4`, `bid_v_5`
* Asks: `ask_v_1`, `ask_v_2`, `ask_v_3`, `ask_v_4`, `ask_v_5`

---

## 3. Heuristic and Machine Learning Integration

While the pure mathematical formulation of the $I$ metric indicates imbalance, static thresholds alone are prone to false positives due to natural large blocks or institutional execution strategies. To counter this, the `[[concepts/spoofing-detection]]` pipeline pairs $I$ with a high velocity of cancellations.

### 3.1 The Dual-Heuristic Trigger
In the standard heuristic implementation, a potential `SPOOFING_SUSPICION` is flagged only if both of the following conditions are met:

$$\Big( |I| > \theta_{\text{imbalance}} \Big) \;\land\; \Big( C_r > \theta_{\text{cancel}} \Big)$$

Where:
* $\theta_{\text{imbalance}}$ is the configured imbalance threshold (e.g., $0.80$).
* $C_r$ is the cancellation rate representing the frequency of cancelled orders in the localized time window (e.g., last $1000\text{ ms}$).
* $\theta_{\text{cancel}}$ is the cancellation threshold (e.g., $0.90$).

This logical conjunction ensures that passive, genuine large-size institutional orders (which sit in the book without high-velocity cancellations) do not trigger false alerts.

### 3.2 Machine Learning Feature Space
In the production pipeline, the multi-level orderbook array is not compressed into a single scalar value. Instead, the individual features are fed as a multidimensional vector to time-series deep-learning models:

$$\mathbf{x}_t = [V_{b,1}, V_{b,2}, V_{b,3}, V_{b,4}, V_{b,5}, V_{a,1}, V_{a,2}, V_{a,3}, V_{a,4}, V_{a,5}, C_r]_t$$

This sequential vector series $\mathbf{X} = [\mathbf{x}_{t-W}, \dots, \mathbf{x}_t]$ (where $W$ is a sliding time window) is processed by an **LSTM Autoencoder** to recognize spatial-temporal patterns that indicate manipulative orderbook pressure. Refer to the model lineage in `[[summaries/project-astrophage]]` for more context.

---

## 4. Reference Implementation

Below is the concrete mathematical implementation abstracted from `src/models/spoofing_detector.py` showing the usage of `pandas` vectors to calculate L1-L5 imbalances across streaming snapshots:

```python
import pandas as pd
import numpy as np

def calculate_l1_l5_imbalance(row: pd.Series, epsilon: float = 1e-9) -> float:
    """
    Computes the L1-L5 orderbook imbalance using the mathematical formulation:
    Imbalance = (Sum(Bid_V) - Sum(Ask_V)) / (Sum(Bid_V) + Sum(Ask_V) + epsilon)
    """
    # Extract bid and ask volumes for levels 1 through 5
    total_bid = sum([row[f'bid_v_{i}'] for i in range(1, 6)])
    total_ask = sum([row[f'ask_v_{i}'] for i in range(1, 6)])
    
    # Mathematical normalization step
    imbalance = (total_bid - total_ask) / (total_bid + total_ask + epsilon)
    return imbalance
```

This computation adheres to the principles detailed in `[[decisions/decoupling-analytics-transport]]`. By keeping the calculation fully isolated from transport classes and streaming brokers, it can be tested offline against historical market replay files or generated unit testing frameworks.