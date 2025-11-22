import json
import pg8000
import boto3
from datetime import datetime

def get_secret(secret_name):
    """Busca secret do Secrets Manager"""
    secrets_client = boto3.client('secretsmanager')
    try:
        response = secrets_client.get_secret_value(SecretId=secret_name)
        return json.loads(response['SecretString'])
    except Exception as e:
        print(f"Erro ao buscar secret {secret_name}: {str(e)}")
        raise

def get_last_prediction():
    """Busca a última previsão da tabela predictions1"""
    db_secret = get_secret('citrus_edge/citrus_edge_db')
    
    conn = pg8000.connect(
        host="citrus-edge-db-instance-1.cywyalcolhuz.us-east-1.rds.amazonaws.com",
        port=5432,
        database="citrusedge",
        user=db_secret['username'],
        password=db_secret['password']
    )
    
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT dia_previsto, score, sistema
                FROM predictions1
                ORDER BY dia_previsto DESC, created_at DESC
                LIMIT 1
            """)
            
            result = cur.fetchone()
            if result:
                return {
                    'dia_previsto': result[0].isoformat() if hasattr(result[0], 'isoformat') else str(result[0]),
                    'score': float(result[1]),
                    'sistema': result[2]
                }
            return None
    finally:
        conn.close()

def send_sms(phone_number, message):
    """Envia SMS via SNS"""
    sns_client = boto3.client('sns')
    
    try:
        response = sns_client.publish(
            PhoneNumber=phone_number,
            Message=message
        )
        print(f"SMS enviado com sucesso. MessageId: {response['MessageId']}")
        return response
    except Exception as e:
        print(f"Erro ao enviar SMS: {str(e)}")
        raise

def format_message(score, dia_previsto):
    """Formata a mensagem do SMS"""
    score_percent = score * 100
    message = f"A propensão de amanhã ser um bom dia para pulverizar é: {score_percent:.1f}%"
    
    return message

def lambda_handler(event, context):
    """Handler principal da Lambda"""
    
    try:
        print("Iniciando notificação SMS...")
        
        prediction = get_last_prediction()
        
        if not prediction:
            return {
                'statusCode': 404,
                'body': json.dumps({'error': 'Nenhuma previsão encontrada'})
            }
        
        phone_number = "+5511976805886"
        
        message = format_message(
            prediction['score'],
            prediction['dia_previsto']
        )
        
        print(f"Enviando SMS para {phone_number}")
        print(f"Mensagem: {message}")
        
        send_sms(phone_number, message)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'SMS enviado com sucesso',
                'dia_previsto': prediction['dia_previsto'],
                'score': prediction['score'],
                'timestamp': datetime.now().isoformat()
            })
        }
        
    except Exception as e:
        print(f"Erro no processamento: {str(e)}")
        import traceback
        traceback.print_exc()
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        }

