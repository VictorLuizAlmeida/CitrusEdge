import json
import requests
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

def get_meteorological_data(latitude: float, longitude: float, data_inicio: str, data_fim: str, chave_api: str) -> list:
    """
    Obtém dados meteorológicos horários do Visual Crossing e retorna como lista
    """
    
    url = (
        f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/"
        f"{latitude},{longitude}/{data_inicio}/{data_fim}"
        f"?unitGroup=metric&key={chave_api}&contentType=json&include=hours"
    )
    
    response = requests.get(url) 
    response.raise_for_status()
    dados = response.json()
    
    dados_horarios = []
    
    for dia in dados['days']:
        data_dia = dia['datetime']
        
        for hora in dia.get('hours', []):
            # Só processar dados observados
            if hora.get('source') == 'obs':
                registro = {
                    'timestamp': f"{data_dia} {hora['datetime']}",
                    'temp': convert_to_float(hora.get('temp')),
                    'pressure': convert_to_float(hora.get('pressure')),
                    'humidity': convert_to_float(hora.get('humidity')),
                    'dew': convert_to_float(hora.get('dew')),
                    'windspeed': convert_to_float(hora.get('windspeed')),
                    'winddir': convert_to_float(hora.get('winddir')),
                    'precip': convert_to_float(hora.get('precip')),
                    'visibility': convert_to_float(hora.get('visibility')),
                    'cloudcover': convert_to_float(hora.get('cloudcover')),
                    'source': hora.get('source')
                }
                dados_horarios.append(registro)
    
    # Remover duplicatas baseado no timestamp
    unique_data = {}
    for registro in dados_horarios:
        timestamp = registro['timestamp']
        if timestamp not in unique_data:
            unique_data[timestamp] = registro
    
    return list(unique_data.values())

def convert_to_float(value):
    """Converte valor para float, retorna None se não conseguir"""
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None

def get_last_timestamp_from_db():
    """Busca o último timestamp no banco de dados"""
    # Buscar credenciais do banco do Secrets Manager
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
            cur.execute("SELECT MAX(timestamp) FROM citrus1")
            result = cur.fetchone()[0]
            
            if result:
                # Converter para string YYYY-MM-DD
                return result.strftime('%Y-%m-%d')
            else:
                # Se não tem dados, começar de uma data padrão
                return "2024-01-01"
    finally:
        conn.close()

def insert_data_to_db(data_list):
    """Insere lista de dados no banco com ON CONFLICT"""
    if not data_list:
        return
    
    # Buscar credenciais do banco do Secrets Manager
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
            for record in data_list:
                cur.execute("""
                    INSERT INTO citrus1 (timestamp, temp, pressure, humidity, dew, windspeed, winddir, precip, visibility, cloudcover, source)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (timestamp) DO NOTHING;
                """, [
                    record['timestamp'],
                    record['temp'],
                    record['pressure'], 
                    record['humidity'],
                    record['dew'],
                    record['windspeed'],
                    record['winddir'],
                    record['precip'],
                    record['visibility'],
                    record['cloudcover'],
                    record['source']
                ])
            conn.commit()
    finally:
        conn.close()

def lambda_handler(event, context):
    """Handler principal da Lambda"""
    
    try:
        # Coordenadas fixas 
        latitude = -22.5901
        longitude = -47.4600
        
        # Buscar API key do Secrets Manager
        api_secret = get_secret('citrus_edge/visual_crossing_api_key')
        chave_api = api_secret['visual_crossing'] 
        
        # 1. Buscar última data no banco
        data_inicio = get_last_timestamp_from_db()
        
        # 2. Data de hoje
        data_fim = datetime.now().strftime('%Y-%m-%d')
        
        print(f"Buscando dados de {data_inicio} até {data_fim}")
        
        # 3. Buscar dados meteorológicos
        weather_data = get_meteorological_data(latitude, longitude, data_inicio, data_fim, chave_api)
        
        if not weather_data:
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Nenhum dado novo encontrado',
                    'period': f"{data_inicio} to {data_fim}"
                })
            }
        
        # 4. Inserir no banco
        insert_data_to_db(weather_data)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Dados inseridos com sucesso',
                'period': f"{data_inicio} to {data_fim}",
                'records_processed': len(weather_data)
            })
        }
        
    except Exception as e:
        print(f"Erro: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        }