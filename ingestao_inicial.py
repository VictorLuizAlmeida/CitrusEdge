import os
import pandas as pd
import requests
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

def get_meteorological_data(latitude: float, longitude: float, data_inicio: str, data_fim: str, chave_api: str) -> pd.DataFrame:
    """
    Obtém dados meteorológicos horários do Visual Crossing e retorna como DataFrame
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
            registro = {
                'timestamp': pd.to_datetime(f"{data_dia} {hora['datetime']}"),
                'temp': hora.get('temp'),
                'humidity': hora.get('humidity'),
                'dew': hora.get('dew'),
                'precip': hora.get('precip'),
                'windspeed': hora.get('windspeed'),
                'winddir': hora.get('winddir'),
                'pressure': hora.get('pressure'),
                'visibility': hora.get('visibility'),
                'cloudcover': hora.get('cloudcover'),
                'conditions': hora.get('conditions'),
                'source': hora.get('source')
            }
            dados_horarios.append(registro)
    
    df = pd.DataFrame(dados_horarios)[["timestamp", "temp","pressure","humidity","dew","windspeed","winddir","precip","visibility","cloudcover","source"]]
    
    df = df[df["source"] ==  "obs"]

    for col in ["temp","pressure","humidity","dew","windspeed","winddir","precip","visibility","cloudcover"]:
        df[col] = pd.to_numeric(df[col], errors = 'coerce')

    df = df.drop_duplicates(subset=['timestamp'])

    return df

chave_api = os.getenv('VISUAL_CROSSING_API_KEY')
df_clima = get_meteorological_data(latitude = "-22.5901", longitude = -47.4600, data_inicio = "2025-08-14", data_fim =  "2025-08-14", chave_api=chave_api)

engine = create_engine("postgresql://citrusedge:dF2!6aNVZb1@citrus-edge-db-instance-1.cywyalcolhuz.us-east-1.rds.amazonaws.com:5432/citrusedge")

df_clima.to_sql('citrus1', engine, if_exists='append', index=False, method='multi')

