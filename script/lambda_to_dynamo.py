import json
import boto3
import csv
import uuid
import logging
from decimal import Decimal
from datetime import datetime

# Configuración del logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('parcial_bryan')

def safe_int(value):
    try:
        return int(float(value)) if value else 0
    except ValueError:
        return 0

def safe_decimal(value):
    try:
        return Decimal(str(value)) if value else Decimal('0.0')
    except ValueError:
        return Decimal('0.0')

def lambda_handler(event, context):
    logger.info("Lambda function started")

    records = event['Records'][0]
    s3bucket = records['s3']['bucket']['name']
    file = records['s3']['object']['key']

    try:
        obj = s3.get_object(Bucket=s3bucket, Key=file)
        data = obj['Body'].read().decode('utf-8').splitlines()

        records = csv.reader(data)
        header = next(records)

        for row in records:
            if len(row) < 9:
                logger.warning(f"Fila incompleta, se asignarán valores predeterminados: {row}")
            
            # Convertir UUID a bytes si la clave primaria es tipo Binary
            id_value = uuid.uuid4().bytes

            table.put_item(
                Item={
                    "id": id_value,  # Usar UUID en formato binario
                    "site_id": str(row[0]) if len(row) > 0 and row[0] else "missing",
                    "timestamp": str(row[1]) if len(row) > 1 and row[1] else "missing",
                    "air_temperature": safe_decimal(row[2]) if len(row) > 2 else Decimal('0.0'),
                    "cloud_coverage": safe_int(row[3]) if len(row) > 3 else 0,
                    "dew_temperature": safe_decimal(row[4]) if len(row) > 4 else Decimal('0.0'),
                    "precip_depth_1_hr": safe_decimal(row[5]) if len(row) > 5 else Decimal('0.0'),
                    "sea_level_pressure": safe_decimal(row[6]) if len(row) > 6 else Decimal('0.0'),
                    "wind_direction": safe_int(row[7]) if len(row) > 7 else 0,
                    "wind_speed": safe_decimal(row[8]) if len(row) > 8 else Decimal('0.0')
                }
            )

        logger.info("El archivo se procesó correctamente")

    except Exception as e:
        logger.error(f"Error al procesar el archivo: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps('Error al agregar data')
        }

    return {
        'statusCode': 200,
        'body': json.dumps('Archivo procesado correctamente')
    }
