import pandas as pd
import matplotlib.pyplot as plt

LINEAR_FILE = "evaluation/linear_alerts.csv"
CENTRALITY_FILE = "evaluation/centrality_alerts.csv"

print("[Evaluation] Loading CSV files...")

linear = pd.read_csv(LINEAR_FILE)
centrality = pd.read_csv(CENTRALITY_FILE)

print("\n===== ALERT COUNT =====")
print("Linear:", len(linear))
print("Centrality:", len(centrality))

print("\n===== UNIQUE NODES ALERTED =====")
print("Linear:", linear["node_id"].nunique())
print("Centrality:", centrality["node_id"].nunique())

print("\n===== RISK SCORE STATS =====")
print("\nLinear:\n", linear["risk_score"].describe())
print("\nCentrality:\n", centrality["risk_score"].describe())

# ---------- Alerts per node ----------
plt.figure()
linear.groupby("node_id").size().plot(
    kind="hist", bins=20, alpha=0.7, label="Linear"
)
centrality.groupby("node_id").size().plot(
    kind="hist", bins=20, alpha=0.7, label="Centrality"
)
plt.legend()
plt.title("Alert Frequency per Node")
plt.xlabel("Alerts per Node")
plt.ylabel("Count")
plt.tight_layout()
plt.show()

# ---------- Risk score distribution ----------
plt.figure()
plt.hist(linear["risk_score"], bins=20, alpha=0.7, label="Linear")
plt.hist(centrality["risk_score"], bins=20, alpha=0.7, label="Centrality")
plt.legend()
plt.title("Risk Score Distribution")
plt.xlabel("Risk Score")
plt.ylabel("Frequency")
plt.tight_layout()
plt.show()

