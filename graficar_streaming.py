# Grafica el ultimo micro-lote guardado por spark_streaming_consumer_v2.py
import glob
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

ultimo = sorted(glob.glob("resultados_streaming/lote_*.csv"))[-1]
df = pd.read_csv(ultimo, parse_dates=["inicio"])
print("Archivo:", ultimo, "| filas:", len(df))

fig, axes = plt.subplots(1, 2, figsize=(13, 5))
for sensor, g in df.groupby("sensor_id"):
    axes[0].plot(g["inicio"].dt.strftime("%H:%M"), g["temp_prom"], marker="o", label=f"Sensor {sensor}")
axes[0].set_title("Temperatura promedio por ventana de 1 minuto")
axes[0].set_xlabel("Inicio de la ventana"); axes[0].set_ylabel("°C")
axes[0].legend(fontsize=7, ncol=2)

tot = df.groupby("sensor_id")[["lecturas", "alertas"]].sum()
axes[1].bar(tot.index.astype(str), tot["lecturas"], label="Lecturas", color="#4C72B0")
axes[1].bar(tot.index.astype(str), tot["alertas"], label="Alertas > 28 °C", color="#C44E52")
axes[1].set_title("Lecturas y alertas acumuladas por sensor")
axes[1].set_xlabel("Sensor"); axes[1].legend()
plt.tight_layout(); plt.savefig("fig_streaming.png", dpi=200)
print("Grafica guardada en fig_streaming.png")
