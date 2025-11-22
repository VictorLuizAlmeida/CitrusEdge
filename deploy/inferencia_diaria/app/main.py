import json
import pickle
import pg8000
import boto3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from catboost import Pool
import decimal


def get_secret(secret_name):
    """Busca secret do Secrets Manager"""
    secrets_client = boto3.client('secretsmanager')
    try:
        response = secrets_client.get_secret_value(SecretId=secret_name)
        return json.loads(response['SecretString'])
    except Exception as e:
        print(f"Erro ao buscar secret {secret_name}: {str(e)}")
        raise

def get_data_from_db():
    """Busca todos os dados da tabela citrus1"""
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
                SELECT timestamp, temp, pressure, humidity, dew, 
                       windspeed, winddir, precip, visibility, cloudcover
                FROM citrus1
                ORDER BY timestamp
            """)
            
            columns = [desc[0] for desc in cur.description]
            data = cur.fetchall()
            df = pd.DataFrame(data, columns=columns)
            
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df.index = df['timestamp']
            
            return df
    finally:
        conn.close()

def create_predictions_table_if_not_exists():
    """Cria a tabela predictions1 se ela não existir"""
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
                CREATE TABLE IF NOT EXISTS predictions1 (
                    dia_previsto DATE NOT NULL,
                    sistema VARCHAR(100) NOT NULL,
                    score FLOAT,
                    features JSONB,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (dia_previsto, sistema)
                )
            """)
            conn.commit()
            print("Tabela predictions1 criada/verificada com sucesso")
    finally:
        conn.close()

def insert_prediction(dia_previsto, sistema, score, features_dict):
    """Insere previsão na tabela predictions1"""
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
            def json_converter(o):
                if isinstance(o, (np.float32, np.float64, np.int32, np.int64)):
                    return float(o)
                if isinstance(o, decimal.Decimal):
                    return float(o)
                if isinstance(o, (pd.Timestamp, datetime)):
                    return o.isoformat()
                raise TypeError(f"Tipo não serializável: {type(o)}")

            features_json = json.dumps(features_dict, default=json_converter)
            
            cur.execute("""
                INSERT INTO predictions1 (dia_previsto, sistema, score, features)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (dia_previsto, sistema) 
                DO UPDATE SET 
                    score = EXCLUDED.score,
                    features = EXCLUDED.features,
                    created_at = CURRENT_TIMESTAMP
            """, [dia_previsto, sistema, score, features_json])
            
            conn.commit()
            print(f"Previsão inserida: {dia_previsto} - Sistema: {sistema} - Score: {score}")
    finally:
        conn.close()

def make_lags(df2, lags_hours, tag):
    """Cria features de lag para horas específicas no passado"""
    cols = ['temp', 'pressure', 'humidity', 'dew', 'windspeed',
            'winddir', 'precip', 'visibility', 'cloudcover']
    
    lag_features = []
    
    for lag in lags_hours:
        lagged = df2[cols].shift(lag).add_suffix(f'_{tag}_lag{lag}h')
        lag_features.append(lagged)
    
    return pd.concat(lag_features, axis=1)

def main():
    try:
        print("Iniciando processamento pulverizar_c1_v0")
        
        create_predictions_table_if_not_exists()
        
        print("Carregando modelo CatBoost Classifier...")
        with open("cb_v0.pkl", "rb") as f:
            cb = pickle.load(f)
        
        print("Buscando dados da tabela citrus1...")
        df = get_data_from_db()
        if df.empty:
            print("Nenhum dado encontrado na tabela citrus1")
            return
        
        df2 = df.sort_index()
        cutoff = pd.Timestamp.now() - pd.Timedelta(hours=80)
        df_last_80 = df2.loc[df2.index >= cutoff]
        if df_last_80.empty:
            print("Dados insuficientes nas últimas 80 horas")
            return
        
        print("Criando features de lag...")
        lags_curtos = [1, 3, 6]
        lags_longos = [12, 18, 24, 48, 72]
        feat_lag_short = make_lags(df_last_80, lags_curtos, 'short')
        feat_lag_long = make_lags(df_last_80, lags_longos, 'long')
        features_full = pd.concat([feat_lag_short, feat_lag_long], axis=1)
        
        features = features_full[features_full.index.hour == 15].reset_index()
        if features.empty:
            print("Nenhum dado disponível às 15h")
            return
        
        dia_em_avaliacao = features.iloc[[-1]].copy()
        
        features_names = [
            'temp_short_lag1h', 'pressure_short_lag1h', 'humidity_short_lag1h', 'dew_short_lag1h', 
            'windspeed_short_lag1h', 'winddir_short_lag1h', 'precip_short_lag1h', 'visibility_short_lag1h', 
            'cloudcover_short_lag1h', 'temp_short_lag3h', 'pressure_short_lag3h', 'humidity_short_lag3h', 
            'dew_short_lag3h', 'windspeed_short_lag3h', 'winddir_short_lag3h', 'precip_short_lag3h', 
            'visibility_short_lag3h', 'cloudcover_short_lag3h', 'temp_short_lag6h', 'pressure_short_lag6h', 
            'humidity_short_lag6h', 'dew_short_lag6h', 'windspeed_short_lag6h', 'winddir_short_lag6h', 
            'precip_short_lag6h', 'visibility_short_lag6h', 'cloudcover_short_lag6h', 'temp_long_lag12h', 
            'pressure_long_lag12h', 'humidity_long_lag12h', 'dew_long_lag12h', 'windspeed_long_lag12h', 
            'winddir_long_lag12h', 'precip_long_lag12h', 'visibility_long_lag12h', 'cloudcover_long_lag12h', 
            'temp_long_lag18h', 'pressure_long_lag18h', 'humidity_long_lag18h', 'dew_long_lag18h', 
            'windspeed_long_lag18h', 'winddir_long_lag18h', 'precip_long_lag18h', 'visibility_long_lag18h', 
            'cloudcover_long_lag18h', 'temp_long_lag24h', 'pressure_long_lag24h', 'humidity_long_lag24h', 
            'dew_long_lag24h', 'windspeed_long_lag24h', 'winddir_long_lag24h', 'precip_long_lag24h', 
            'visibility_long_lag24h', 'cloudcover_long_lag24h', 'temp_long_lag48h', 'pressure_long_lag48h', 
            'humidity_long_lag48h', 'dew_long_lag48h', 'windspeed_long_lag48h', 'winddir_long_lag48h', 
            'precip_long_lag48h', 'visibility_long_lag48h', 'cloudcover_long_lag48h', 'temp_long_lag72h', 
            'pressure_long_lag72h', 'humidity_long_lag72h', 'dew_long_lag72h', 'windspeed_long_lag72h', 
            'winddir_long_lag72h', 'precip_long_lag72h', 'visibility_long_lag72h', 'cloudcover_long_lag72h'
        ]
        
        print("Fazendo previsão com CatBoost...")
        pred_pool = Pool(data=dia_em_avaliacao[features_names])
        prediction = float(cb.predict_proba(pred_pool)[:, 1][0])
        print("Previsão feita com sucesso")
        print(prediction)
        
        features_object = dia_em_avaliacao[features_names].iloc[0].to_dict()
        features_object = {k: (None if pd.isna(v) else v) for k, v in features_object.items()}
        
        sistema = "pulverizar_c1_v0"
        dia_previsto = (datetime.now() + timedelta(days=1)).date()
        
        print(f"Salvando previsão para {dia_previsto}...")
        insert_prediction(dia_previsto, sistema, prediction, features_object)
        
        print("Previsão realizada com sucesso")
        print({
            'dia_previsto': str(dia_previsto),
            'sistema': sistema,
            'score': prediction,
            'timestamp_processamento': datetime.now().isoformat()
        })
        
    except Exception as e:
        print(f"Erro no processamento: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
