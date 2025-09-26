import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, regexp_replace, sum as _sum, count as _count, avg as _avg, to_date, datediff, lit
from pyspark.sql.types import StructType, StructField, StringType

# Argumentos vindos da Lambda
args = getResolvedOptions(sys.argv, ["JOB_NAME", "DATA_EXECUCAO", "DATA_EXECUCAO_D2"])

# Criação dos contextos
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Partições recebidas
data_execucao = args["DATA_EXECUCAO"]
data_execucao_d2 = args["DATA_EXECUCAO_D2"]

print(f"Executando para D0: {data_execucao}, D-2: {data_execucao_d2 if data_execucao_d2 else 'não enviado'}")

# Caminho no S3 para D0
path_d0 = f"s3://bucket-raw-data-tech2/dados_b3/dt_ptcm={data_execucao}/dados_b3_{data_execucao}.parquet"

# ---------------------------
# Função utilitária para renomear colunas
# ---------------------------
def rename_columns(df: DataFrame, rename_map: dict) -> DataFrame:
    for old_col, new_col in rename_map.items():
        if old_col in df.columns:
            df = df.withColumnRenamed(old_col, new_col)
    return df

# ---------------------------
# Leitura da partição D0
# ---------------------------
df_d0 = spark.read.parquet(path_d0)

# Leitura da partição D-2 (se existir)
if data_execucao_d2:
    path_d2 = f"s3://bucket-raw-data-tech2/dados_b3/dt_ptcm={data_execucao_d2}/dados_b3_{data_execucao_d2}.parquet"
    try:
        df_d2 = spark.read.parquet(path_d2)
    except Exception as e:
        print(f"Aviso: não foi possível ler D-2 ({data_execucao_d2}). Erro: {e}")
        df_d2 = spark.createDataFrame([], schema=df_d0.schema)  # cria DF vazio com mesmo schema
else:
    print("Nenhuma data D-2 enviada. Criando DataFrame vazio.")
    df_d2 = spark.createDataFrame([], schema=df_d0.schema)

# ---------------------------
# Requisito 5.B: renomeando colunas
# ---------------------------
rename_map = {
    "Código": "codigo",
    "Ação": "acao",
    "Tipo": "tipo",
    "Qtde. Teórica": "qt_teorica",
    "Part. (%)": "pc_participacao"
}

df_d0 = rename_columns(df_d0, rename_map)
df_d2 = rename_columns(df_d2, rename_map)

# Normalizando colunas numéricas D0
df_d0 = df_d0.withColumn("qt_teorica", regexp_replace(col("qt_teorica"), r"\.", "").cast("long"))
df_d0 = df_d0.withColumn("pc_participacao", regexp_replace(col("pc_participacao"), ",", ".").cast("double"))

# Normalizando colunas numéricas D-2
if df_d2.columns:  # só aplica se não for vazio
    df_d2 = df_d2.withColumn("qt_teorica", regexp_replace(col("qt_teorica"), r"\.", "").cast("long"))
    df_d2 = df_d2.withColumn("pc_participacao", regexp_replace(col("pc_participacao"), ",", ".").cast("double"))

print("Esquema D0: ")
df_d0.printSchema()
df_d0.show(5)

print("Esquema D-2:")
df_d2.printSchema()
df_d2.show(5)

# diretório S3 para salvar os dados brutos particionados
raw_data_path_d0 = "s3://bucket-refined-data-tech2/data_execucao_d0"
raw_data_path_d2 = "s3://bucket-refined-data-tech2/data_execucao_d2"

# salva os dados brutos em formato parquet no S3
df_d0.write.mode("overwrite").parquet(raw_data_path_d0)
df_d2.write.mode("overwrite").parquet(raw_data_path_d2)

