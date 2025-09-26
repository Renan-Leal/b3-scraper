import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, sum as _sum, avg as _avg, count as _count

# ---------------------------
# Configuração básica do Glue
# ---------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ---------------------------
# Leitura dos dados D0 e D-2
# ---------------------------
path_d0 = "s3://bucket-refined-data-tech2/data_execucao_d0/"
path_d2 = "s3://bucket-refined-data-tech2/data_execucao_d2/"

df_d0 = spark.read.parquet(path_d0)
df_d2 = spark.read.parquet(path_d2)

print("Schema D0:")
df_d0.printSchema()
print("Schema D-2:")
df_d2.printSchema()

# ---------------------------
# Requisito 5.A: Agrupamento em D0
# ---------------------------
df_agg_d0 = (
    df_d0.groupBy("codigo", "acao", "dt_ptcm")
    .agg(
        _sum("qt_teorica").alias("soma_qt_teorica"),
        _avg("pc_participacao").alias("media_pc_participacao"),
        _count("*").alias("qtd_registros")
    )
)

print("Resultado do agrupamento (5.A):")
df_agg_d0.show(5, truncate=False)

# ---------------------------
# Requisito 5.C: Comparação entre D0 e D-2
# ---------------------------
df_comparacao = (
    df_d0.alias("d0")
    .join(
        df_d2.alias("d2"),
        on=["codigo", "acao"],  # join por código e nome da ação
        how="left"              # mantém os de D0, mesmo que não existam em D-2
    )
    .select(
        col("codigo"),
        col("acao"),
        col("d0.dt_ptcm"),
        (col("d0.qt_teorica") - col("d2.qt_teorica")).alias("diferenca_qt_teorica"),
        (col("d0.pc_participacao") - col("d2.pc_participacao")).alias("diferenca_pc_participacao")
    )
)

print("Comparação D0 x D-2 (5.C):")
df_comparacao.show(5, truncate=False)

# ---------------------------
# Salvar resultados no S3
# ---------------------------
output_base = "s3://bucket-refined-data-tech2/output_tc_transform/"

df_agg_d0.write.mode("overwrite").parquet(output_base + "agrupamento_d0/")
df_comparacao.write.mode("overwrite").parquet(output_base + "comparacao_d0_d2/")

# ---------------------------
# Finalização do job
# ---------------------------
job.commit()

