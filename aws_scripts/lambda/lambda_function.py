import boto3
import os
import json

# Inicializa os clientes da AWS
s3 = boto3.client("s3")
sfn = boto3.client("stepfunctions")

# Nome da state machine (recuperado de uma variável de ambiente configurada na Lambda)
STATE_MACHINE_ARN = os.environ.get("STATE_MACHINE_ARN")

# Configuração do bucket e do prefixo onde estão os dados particionados
BUCKET = "bucket-raw-data-tech2"
PREFIX = "dados_b3/"

def lambda_handler(event, context):    
    print("Evento recebido:", event)

    try:
        # Lista os objetos no bucket dentro do prefixo especificado, retornando apenas "subpastas" devido ao uso de Delimiter="/"
        response = s3.list_objects_v2(Bucket=BUCKET, Prefix=PREFIX, Delimiter="/")

        datas = []

        # Itera sobre os "CommonPrefixes", que representam cada subpasta encontrada
        for cp in response.get("CommonPrefixes", []):
            prefix = cp.get("Prefix").strip("/")      # Remove barra final
            part = prefix.split("/")[-1]             # Pega somente o último nível da partição

            # Verifica se a pasta segue o padrão dt_ptcm=YYYYMMDD
            if part.startswith("dt_ptcm="):
                data_str = part.replace("dt_ptcm=", "")  # Extrai apenas a parte da data

                # Valida se é um número de 8 dígitos (YYYYMMDD)
                if data_str.isdigit() and len(data_str) == 8:
                    datas.append(data_str)

        # Se não encontrar nenhuma partição válida, lança erro
        if not datas:
            raise Exception("Nenhuma partição encontrada em " + PREFIX)

        # Ordena as datas encontradas e pega a mais recente (última)
        datas = sorted(datas)
        ultima_data = datas[-1]  # D0
        d_menos_2 = datas[-3] if len(datas) >= 3 else ""  # D-2
        
        print("Última partição (D0):", ultima_data)
        print("Partição D-2:", d_menos_2 if d_menos_2 else "não encontrada")

        # Monta os argumentos que serão repassados ao Step Functions, e depois utilizados pelos Glue Jobs
        arguments = {
            "--DATA_EXECUCAO": ultima_data,
            "--DATA_EXECUCAO_D2": d_menos_2
        }

        # Inicia a execução da state machine no Step Functions, passando os argumentos como input em formato JSON
        response = sfn.start_execution(
            stateMachineArn=STATE_MACHINE_ARN,
            input=json.dumps({"arguments": arguments})
        )

        print("Step Function iniciado:", response)

        # Retorno de sucesso da Lambda, com informações da execução iniciada
        return {
            "statusCode": 200,
            "body": f"Step Function iniciado. Execução: {response['executionArn']}, "
                    f"D0: {ultima_data}, D-2: {d_menos_2 if d_menos_2 else 'vazio'}"
        }

    except Exception as e:
        # Captura qualquer erro e retorna com status 500
        print("Erro ao iniciar o Step Function:", str(e))
        return {
            "statusCode": 500,
            "body": f"Erro: {str(e)}"
        }
