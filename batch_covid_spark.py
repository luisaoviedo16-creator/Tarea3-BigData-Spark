# -*- coding: utf-8 -*-
"""
Tarea 3 - Big Data (202016911) UNAD
Procesamiento en batch con Apache Spark (PySpark)
Estudiante: Nestor Miguel Pacheco Ferrer - 1129502093

Conjunto de datos: COVID-19 de Our World in Data (OWID)
Fuente: https://github.com/owid/covid-19-data
Ejecucion en la maquina virtual:
    wget https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv
    spark-submit batch_covid_spark.py
"""
import os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

RUTA = os.environ.get("RUTA_DATOS", "owid-covid-data.csv")
SALIDA = os.environ.get("RUTA_SALIDA", "resultados")
os.makedirs(SALIDA, exist_ok=True)

spark = (SparkSession.builder.appName("COVID19_Batch_OWID")
         .config("spark.ui.showConsoleProgress", "false").getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------
# 1. Carga de datos
# ---------------------------------------------------------------
df = spark.read.csv(RUTA, header=True, inferSchema=True)
print("Filas originales:", df.count(), "| Columnas:", len(df.columns))
df.printSchema()

# ---------------------------------------------------------------
# 2. Limpieza
# ---------------------------------------------------------------
cols = ["iso_code", "continent", "location", "date", "population",
        "total_cases", "new_cases", "total_deaths", "new_deaths",
        "total_cases_per_million", "total_deaths_per_million",
        "people_vaccinated_per_hundred", "people_fully_vaccinated_per_hundred",
        "gdp_per_capita", "median_age"]

limpio = (df.select(*cols)
            # quitar agregados de OWID (World, South America, High income, etc.)
            .filter(~F.col("iso_code").startswith("OWID_"))
            .filter(F.col("continent").isNotNull())
            .withColumn("date", F.to_date("date"))
            # valores negativos son correcciones de los reportes: se dejan nulos
            .withColumn("new_cases", F.when(F.col("new_cases") < 0, None).otherwise(F.col("new_cases")))
            .withColumn("new_deaths", F.when(F.col("new_deaths") < 0, None).otherwise(F.col("new_deaths")))
            .dropDuplicates(["iso_code", "date"]))

print("Filas despues de limpieza:", limpio.count())
print("Paises:", limpio.select("location").distinct().count())
limpio.agg(F.min("date").alias("fecha_min"), F.max("date").alias("fecha_max")).show()

# nulos por columna clave
nulos = limpio.select([F.round(F.avg(F.col(c).isNull().cast("int")) * 100, 1).alias(c)
                       for c in ["new_cases", "new_deaths", "total_deaths_per_million",
                                 "people_fully_vaccinated_per_hundred", "gdp_per_capita"]])
print("Porcentaje de nulos por columna:")
nulos.show()

# ---------------------------------------------------------------
# 3. Transformacion
# ---------------------------------------------------------------
limpio = (limpio.withColumn("anio", F.year("date"))
                .withColumn("mes", F.date_format("date", "yyyy-MM")))
limpio.cache()

# ultimo valor disponible por pais
w = Window.partitionBy("location").orderBy(F.col("date").desc())
ultimo = (limpio.filter(F.col("total_deaths_per_million").isNotNull())
                .withColumn("rn", F.row_number().over(w))
                .filter("rn = 1").drop("rn"))
vac_max = (limpio.groupBy("location")
                 .agg(F.max("people_fully_vaccinated_per_hundred").alias("vacunacion_completa_pct")))
resumen_pais = (ultimo.select("location", "continent", "population", "total_cases",
                              "total_deaths", "total_cases_per_million",
                              "total_deaths_per_million", "gdp_per_capita", "median_age")
                      .join(vac_max, "location", "left")
                      .withColumn("letalidad_pct",
                                  F.round(F.when(F.col("total_cases") > 0, F.col("total_deaths") / F.col("total_cases") * 100), 2)))

# ---------------------------------------------------------------
# 4. Analisis exploratorio (EDA)
# ---------------------------------------------------------------
print("Top 10 paises con mas muertes por millon (poblacion > 1 millon):")
top10 = (resumen_pais.filter(F.col("population") > 1_000_000)
                     .orderBy(F.desc("total_deaths_per_million"))
                     .select("location", F.round("total_deaths_per_million", 1).alias("muertes_por_millon"),
                             "letalidad_pct"))
top10.show(10, truncate=False)

print("Resumen por continente:")
por_continente = (limpio.groupBy("continent")
                        .agg(F.sum("new_cases").alias("casos"),
                             F.sum("new_deaths").alias("muertes"))
                        .withColumn("letalidad_pct", F.round(F.when(F.col("casos") > 0, F.col("muertes") / F.col("casos") * 100), 2))
                        .orderBy(F.desc("muertes")))
por_continente.show(truncate=False)

latam = ["Colombia", "Argentina", "Brazil", "Chile", "Peru", "Mexico", "Ecuador"]
print("Colombia frente a otros paises de America Latina:")
comparacion = (resumen_pais.filter(F.col("location").isin(latam))
                           .select("location", "total_cases", "total_deaths",
                                   F.round("total_deaths_per_million", 1).alias("muertes_por_millon"),
                                   "letalidad_pct",
                                   F.round("vacunacion_completa_pct", 1).alias("vacunacion_completa_pct"))
                           .orderBy(F.desc("muertes_por_millon")))
comparacion.show(truncate=False)

# En OWID, Colombia deja de reportar muertes despues del 14/05/2023,
# por eso el analisis de Colombia llega hasta mayo de 2023
colombia = limpio.filter((F.col("location") == "Colombia") & (F.col("date") <= "2023-05-31"))
print("Colombia por anio (hasta mayo de 2023):")
col_anio = (colombia
                  .groupBy("anio")
                  .agg(F.sum("new_cases").alias("casos"), F.sum("new_deaths").alias("muertes"),
                       F.max("people_fully_vaccinated_per_hundred").alias("vacunacion_completa_pct"))
                  .withColumn("letalidad_pct", F.round(F.when(F.col("casos") > 0, F.col("muertes") / F.col("casos") * 100), 2))
                  .orderBy("anio"))
col_anio.show()

col_mes = (colombia.groupBy("mes").agg(F.sum("new_cases").alias("casos"),
                                     F.sum("new_deaths").alias("muertes"))
                 .orderBy("mes"))

# correlacion entre vacunacion y mortalidad (paises > 1 millon de habitantes)
base_corr = resumen_pais.filter((F.col("population") > 1_000_000) &
                                F.col("vacunacion_completa_pct").isNotNull() &
                                (F.col("vacunacion_completa_pct") <= 100))
print("Paises en el analisis de correlacion:", base_corr.count())
print("Correlacion vacunacion completa vs muertes por millon:",
      round(base_corr.stat.corr("vacunacion_completa_pct", "total_deaths_per_million"), 3))
print("Correlacion PIB per capita vs vacunacion completa:",
      round(base_corr.na.drop(subset=["gdp_per_capita"]).stat.corr("gdp_per_capita", "vacunacion_completa_pct"), 3))
print("Correlacion edad mediana vs muertes por millon:",
      round(base_corr.na.drop(subset=["median_age"]).stat.corr("median_age", "total_deaths_per_million"), 3))

# ejemplo con RDD: casos totales por continente con map + reduceByKey
rdd = (limpio.select("continent", "new_cases").na.drop().rdd
             .map(lambda r: (r["continent"], r["new_cases"]))
             .reduceByKey(lambda a, b: a + b))
print("Casos por continente (RDD reduceByKey):")
for continente, casos in sorted(rdd.collect(), key=lambda x: -x[1]):
    print(f"  {continente}: {casos:,.0f}")

# ---------------------------------------------------------------
# 5. Visualizacion
# ---------------------------------------------------------------
pd_mes = col_mes.toPandas()
fig, ax1 = plt.subplots(figsize=(11, 5))
ax1.bar(pd_mes["mes"], pd_mes["casos"], color="#4C72B0", label="Casos nuevos")
ax1.set_ylabel("Casos nuevos por mes")
ax2 = ax1.twinx()
ax2.plot(pd_mes["mes"], pd_mes["muertes"], color="#C44E52", marker="o", ms=3, label="Muertes")
ax2.set_ylabel("Muertes por mes")
ax1.set_xticks(range(0, len(pd_mes), 3))
ax1.set_xticklabels(pd_mes["mes"][::3], rotation=60, fontsize=8)
ax1.set_title("Colombia: casos y muertes mensuales por COVID-19")
fig.legend(loc="upper right", bbox_to_anchor=(0.88, 0.88))
plt.tight_layout(); plt.savefig(f"{SALIDA}/fig_colombia_mensual.png", dpi=200); plt.close()

pd_top = top10.limit(10).toPandas().iloc[::-1]
plt.figure(figsize=(9, 5))
plt.barh(pd_top["location"], pd_top["muertes_por_millon"], color="#55A868")
plt.xlabel("Muertes acumuladas por millón de habitantes")
plt.title("Diez países con mayor mortalidad por COVID-19")
plt.tight_layout(); plt.savefig(f"{SALIDA}/fig_top10_mortalidad.png", dpi=200); plt.close()

pd_lat = comparacion.toPandas().sort_values("muertes_por_millon")
fig, ax = plt.subplots(figsize=(9, 5))
colores = ["#C44E52" if p == "Colombia" else "#8172B2" for p in pd_lat["location"]]
ax.bar(pd_lat["location"], pd_lat["muertes_por_millon"], color=colores)
for i, v in enumerate(pd_lat["vacunacion_completa_pct"]):
    ax.text(i, pd_lat["muertes_por_millon"].iloc[i] + 50, f"{v:.0f}% vac.", ha="center", fontsize=8)
ax.set_ylabel("Muertes por millón de habitantes")
ax.set_title("Mortalidad acumulada y vacunación completa en América Latina")
plt.tight_layout(); plt.savefig(f"{SALIDA}/fig_latam.png", dpi=200); plt.close()

pd_corr = base_corr.select("vacunacion_completa_pct", "total_deaths_per_million", "continent").toPandas()
plt.figure(figsize=(9, 5.5))
for cont, g in pd_corr.groupby("continent"):
    plt.scatter(g["vacunacion_completa_pct"], g["total_deaths_per_million"], s=18, alpha=0.75, label=cont)
plt.xlabel("Población con esquema completo (%)")
plt.ylabel("Muertes acumuladas por millón")
plt.title("Vacunación completa frente a mortalidad por país")
plt.legend(fontsize=8)
plt.tight_layout(); plt.savefig(f"{SALIDA}/fig_dispersion_vacunacion.png", dpi=200); plt.close()

# guardar resultados en CSV para el informe
for nombre, d in {"top10": top10, "continentes": por_continente,
                  "latam": comparacion, "colombia_anio": col_anio}.items():
    d.toPandas().to_csv(f"{SALIDA}/{nombre}.csv", index=False)

spark.stop()
