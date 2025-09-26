import sys
import boto3
import logging
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, sum as _sum, avg as _avg, count as _count

# ---------------------------
# Configuração inicial
# ---------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Evita que as partições sejam apagadas
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ---------------------------
# Caminhos de entrada e saída
# ---------------------------
input_d0 = "s3://bucket-refined-data-tech2/output_tc_transform/agrupamento_d0/"
input_d2 = "s3://bucket-refined-data-tech2/output_tc_transform/comparacao_d0_d2/"
output_final = "s3://bucket-refined-data-tech2/tables/"

# ---------------------------
# Leitura dos dados
# ---------------------------
df_d0 = spark.read.parquet(input_d0)
df_d2 = spark.read.parquet(input_d2)

# ---------------------------
# Escrita dos dados no S3
# ---------------------------

# tb_agrupamento -> com partição
(
    df_d0.write
    .mode("overwrite")
    .partitionBy("dt_ptcm", "acao")
    .parquet(output_final + "agrupamento/")
)

# tb_comparacao -> sem partição
(
    df_d2.write
    .mode("overwrite")
    .parquet(output_final + "comparacao/")
)

# ---------------------------
# Glue Catalog - Criação/Atualização
# ---------------------------
database_name = "db_tech2_refined"
glue_client = boto3.client("glue")

# Garantir que DB existe
try:
    glue_client.get_database(Name=database_name)
except glue_client.exceptions.EntityNotFoundException:
    glue_client.create_database(DatabaseInput={"Name": database_name})
    logger.info(f"Database '{database_name}' criado.")

# ---------------------------
# Definição das tabelas
# ---------------------------
tabelas = [
    {
        "nome": "tb_agrupamento",
        "path": output_final + "agrupamento/",
        "colunas": [
            {"Name": "codigo", "Type": "string"},
            {"Name": "soma_qt_teorica", "Type": "bigint"},
            {"Name": "media_pc_participacao", "Type": "double"},
            {"Name": "qtd_registros", "Type": "bigint"},
        ],
        "particoes": [
            {"Name": "dt_ptcm", "Type": "string"},
            {"Name": "acao", "Type": "string"},
        ]
    },
    {
        "nome": "tb_comparacao",
        "path": output_final + "comparacao/",
        "colunas": [
            {"Name": "codigo", "Type": "string"},
            {"Name": "acao", "Type": "string"},
            {"Name": "data_atual", "Type": "string"},
            {"Name": "diff_qt_teorica", "Type": "bigint"},
            {"Name": "diff_pc_participacao", "Type": "double"},
        ],
        "particoes": []  # sem partição
    }
]

for tbl in tabelas:
    table_input = {
        "Name": tbl["nome"],
        "StorageDescriptor": {
            "Columns": tbl["colunas"],
            "Location": tbl["path"],
            "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                "Parameters": {"serialization.format": "1"}
            }
        },
        "PartitionKeys": tbl["particoes"],
        "TableType": "EXTERNAL_TABLE"
    }

    try:
        glue_client.get_table(DatabaseName=database_name, Name=tbl["nome"])
        glue_client.update_table(DatabaseName=database_name, TableInput=table_input)
        logger.info(f"Tabela '{tbl['nome']}' atualizada no Glue Catalog.")
    except glue_client.exceptions.EntityNotFoundException:
        glue_client.create_table(DatabaseName=database_name, TableInput=table_input)
        logger.info(f"Tabela '{tbl['nome']}' criada no Glue Catalog.")

# ---------------------------
# Reparar partições apenas nas tabelas particionadas
# ---------------------------
for tbl in tabelas:
    if tbl["particoes"]:
        repair_sql = f"MSCK REPAIR TABLE {database_name}.{tbl['nome']}"
        logger.info(f"Executando: {repair_sql}")
        spark.sql(repair_sql)

job.commit()

