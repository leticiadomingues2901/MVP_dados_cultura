# Databricks notebook source
# MAGIC %md
# MAGIC Camada Bronze

# COMMAND ----------


caminho_arquivo = "/Volumes/workspace/default/dados_culturas/Crop_recommendation.csv"

df_bronze = spark.read.format("csv") \
  .option("header", "true") \
  .option("inferSchema", "true") \
  .load(caminho_arquivo)


display(df_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC Camada Prata
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col

# Traduzindo colunas
df_prata = df_bronze \
    .withColumnRenamed("temperature", "temperatura") \
    .withColumnRenamed("humidity", "umidade") \
    .withColumnRenamed("rainfall", "chuva_mm") \
    .withColumnRenamed("label", "cultura") \
    .withColumnRenamed("N", "nitrogenio") \
    .withColumnRenamed("P", "fosforo") \
    .withColumnRenamed("K", "potassio")

# 2. Traduzindo nomes culturas
traducao_culturas = {
    "rice": "arroz",
    "maize": "milho",
    "chickpea": "grao de bico",
    "kidneybeans": "feijao vermelho",
    "lentil": "lentilha",
    "pomegranate": "roma",
    "mango": "manga",
    "grapes": "uva",
    "watermelon": "melancia",
    "muskmelon": "melao",
    "apple": "maca",
    "orange": "laranja",
    "papaya": "mamao",
    "coconut": "coco",
    "cotton": "algodao",
    "coffee": "cafe"
}

# 3. Aplicando a tradução nos dados da coluna
df_prata = df_prata.replace(traducao_culturas, subset=["cultura"])

# 4. Removendo qualquer linha que tenha dados nulos
df_prata = df_prata.dropna()

# 5. Verificando Acurácia e Outliers
print("Verificação de Mínimos e Máximos")
df_bronze.summary("min", "max").display()

# 6. Verificando duplicatas
total_linhas = df_bronze.count()
linhas_distintas = df_bronze.dropDuplicates().count()
duplicatas = total_linhas - linhas_distintas
print(f"Linhas duplicadas encontradas: {duplicatas}")


# 7. Removendo culturas que não são relevantes para nossa região
culturas_remover = ["pigeonpeas", "mothbeans", "mungbean", "blackgram", "jute"]
df_prata = df_prata.filter(~col("cultura").isin(culturas_remover))

# 8. Padronizando os dados numéricos
colunas_numericas = [ 
    "temperatura", "umidade", "ph", "chuva_mm"
]

# Loop para aplicar a padronização
for coluna in colunas_numericas:
    df_prata = df_prata.withColumn(coluna, col(coluna).cast("decimal(18,8)"))

# Resultado
display(df_prata)


# COMMAND ----------

df_prata = df_prata.dropDuplicates()

# Salvando os dados tratados como uma tabela delta
df_prata.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("agricultura_prata")

# COMMAND ----------

# MAGIC %md
# MAGIC Camada Ouro

# COMMAND ----------

from pyspark.sql.functions import avg, round, min, max

# 1. Lendo os dados limpos
df_prata_lido = spark.read.table("agricultura_prata")

# 2. Criando a modelagem ouro
df_ouro = df_prata_lido.groupBy("cultura").agg(
    round(avg("nitrogenio"), 0).alias("media_nitrogenio"),
    round(avg("fosforo"), 0).alias("media_fosforo"),
    round(avg("potassio"), 0).alias("media_potassio"),
    round(avg("temperatura"), 2).alias("media_temperatura"),
    round(avg("chuva_mm"), 2).alias("media_chuva"),
    round(min("ph"), 2).alias("ph_minimo"),
    round(max("ph"), 2).alias("ph_maximo")
)

# 3. Salvando a tabela ouro
df_ouro.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("agricultura_ouro_resumo")

display(df_ouro)

# COMMAND ----------

# MAGIC %md
# MAGIC Quais as faixas médias de temperatura e chuva para Algodão comparado ao Café? 

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC   cultura, 
# MAGIC   media_temperatura, 
# MAGIC   media_chuva
# MAGIC FROM agricultura_gold_resumo
# MAGIC WHERE cultura IN ('algodao', 'cafe')

# COMMAND ----------

# MAGIC %md
# MAGIC Quais culturas exigem, em média, os maiores níveis de Nitrogênio no solo?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   cultura, 
# MAGIC   media_nitrogenio
# MAGIC FROM agricultura_gold_resumo
# MAGIC ORDER BY media_nitrogenio DESC
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC Solos com pH extremamente ácido (abaixo de 5.5) limitam o plantio a quais tipos de culturas?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   cultura, 
# MAGIC   ph_minimo,
# MAGIC   ph_maximo
# MAGIC FROM agricultura_gold_resumo
# MAGIC WHERE ph_minimo < 5.5
# MAGIC ORDER BY ph_minimo ASC

# COMMAND ----------

# MAGIC %md
# MAGIC Quais são as melhores frutas para cultivar no clima tropical?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   cultura, 
# MAGIC   media_temperatura, 
# MAGIC   media_chuva
# MAGIC FROM agricultura_gold_resumo
# MAGIC WHERE cultura IN (
# MAGIC     'maca', 'laranja', 'banana', 'manga', 'uva', 
# MAGIC     'mamao', 'melancia', 'melao', 'roma', 'coco'
# MAGIC )
# MAGIC   AND media_temperatura > 25
# MAGIC   AND media_chuva > 100
# MAGIC ORDER BY media_temperatura DESC

# COMMAND ----------

# MAGIC %md
# MAGIC Qual cultura exige o maior investimento financeiro em adubação?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   cultura, 
# MAGIC   (media_nitrogenio + media_fosforo + media_potassio) AS exigencia_total_npk
# MAGIC FROM agricultura_gold_resumo
# MAGIC ORDER BY exigencia_total_npk DESC
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC Em um cenário de crise hídrica ou em regiões áridas, quais são as culturas mais seguras para se investir (aquelas que sobrevivem com menos de 60 mm de chuva)?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   cultura, 
# MAGIC   media_chuva, 
# MAGIC   media_temperatura
# MAGIC FROM agricultura_gold_resumo
# MAGIC WHERE media_chuva < 60
# MAGIC ORDER BY media_chuva ASC