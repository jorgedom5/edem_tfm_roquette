from flask import Flask, jsonify
from flasgger import Swagger, swag_from
from google.cloud import bigquery
from google.cloud import storage
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import joblib
from sklearn.preprocessing import StandardScaler
import os
import json

app = Flask(__name__)
swagger = Swagger(app)

# Configuración de BigQuery
project_id = "banded-setting-428309-q4"
dataset_id = "datos"

# Configuración de Cloud Storage
BUCKET_NAME = 'model_roquette'
MODEL_DIRECTORY = 'models'  # Carpeta en el bucket donde están los modelos

# Función para descargar archivos desde Cloud Storage
def download_blob(bucket_name, source_blob_name, destination_file_name):
    """Descarga un archivo desde Cloud Storage."""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(source_blob_name)
        blob.download_to_filename(destination_file_name)
        print(f'Archivo descargado desde {source_blob_name} en {destination_file_name}')
    except Exception as e:
        print(f'Error al descargar el archivo {source_blob_name}: {str(e)}')
        raise  # Re-raise the exception to halt execution if download fails

# Descargar los modelos desde Cloud Storage al directorio local
model_filename = 'rf_model.pkl'
scaler_filename = 'scaler_model.pkl'

download_blob(BUCKET_NAME, f'{MODEL_DIRECTORY}/{model_filename}', model_filename)
download_blob(BUCKET_NAME, f'{MODEL_DIRECTORY}/{scaler_filename}', scaler_filename)

# Cargar el modelo y el scaler guardados localmente
loaded_model = joblib.load(model_filename)
scaler = joblib.load(scaler_filename)

# Obtén los nombres de las características del scaler entrenado
trained_feature_names = scaler.feature_names_in_

@app.route('/results', methods=['GET'])
@swag_from('swagger.yml')
def get_last_week_data():
    client = bigquery.Client()
    one_week_ago = datetime.now() - timedelta(days=7)

    query = f"""
    SELECT
        FORMAT_TIMESTAMP('%Y-%m-%d', Timestamp) AS Day,
        FORMAT_TIMESTAMP('%H', Timestamp) AS Hour,
        FORMAT_TIMESTAMP('%M', Timestamp) AS Minute,
        ct.descripcion,
        bd.Value
    FROM `banded-setting-428309-q4.datos.bronze-data` bd
    LEFT JOIN `banded-setting-428309-q4.datos.col-tag` ct on bd.Tag = ct.tag
    WHERE DATE(Timestamp) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 50 DAY) AND CURRENT_DATE()
    """

    query_job = client.query(query)
    results = query_job.result()

    data = []
    for row in results:
        data.append(dict(row))

    df = pd.DataFrame(data)

    df_max_values = df.groupby(["descripcion", "Day", "Hour", "Minute"]).agg({"Value": "max"}).reset_index()

    df_max_values['dayhourminute'] = df_max_values['Day'] + ' ' + df_max_values['Hour'] + ':' + df_max_values['Minute']
    df_unpivot = df_max_values.pivot_table(index="dayhourminute", columns="descripcion", values="Value", aggfunc="max").reset_index()

    col_drop = ['COT AGUAS ÁCIDAS NUEVO', 'COT AGUAS ÁCIDAS', 'COR TITÁNIC AZÚCARES', 'COT TITÁNIC AZÚCARES NUEVO', 'dayhourminute']
    df = df_unpivot.drop(columns=[col for col in col_drop if col in df_unpivot.columns])
    df = df.fillna(method='ffill').fillna(0)

    days_hours = df_unpivot[['dayhourminute']]

    for col in trained_feature_names:
        if col not in df.columns:
            df[col] = 0

    df = df[trained_feature_names]

    X = df
    X_scaled = scaler.transform(X)

    probabilities = loaded_model.predict_proba(X_scaled)[:, 1]

    feature_importances = loaded_model.feature_importances_
    influences = feature_importances * X_scaled

    results = []
    for i in range(len(probabilities)):
        feature_values = X_scaled[i]
        influence = feature_importances * feature_values
        top_5_indices = np.argsort(np.abs(influence))[-5:]
        top_5_features = X.columns[top_5_indices]
        top_5_influences = influence[top_5_indices]
        top_5 = pd.DataFrame({
            'Feature': top_5_features,
            'Influence': top_5_influences
        }).sort_values(by='Influence', ascending=False)

        result = {
            "DayHourMinute": days_hours.iloc[i]['dayhourminute'],
            "Probability that flag is 1": probabilities[i],
            "Top 5 features influencing this probability": top_5.to_dict(orient='records')
        }
        results.append(result)

    results_df = pd.DataFrame(results)
    
    results_df["Top 5 features influencing this probability"] = results_df["Top 5 features influencing this probability"].apply(lambda x: json.dumps(x))
    
    def extract_features(features_json, position):
        features_list = json.loads(features_json)
        if len(features_list) > position:
            return features_list[position]['Feature']
        return None

    for i in range(5):
        results_df[f'Top {i+1}'] = results_df["Top 5 features influencing this probability"].apply(lambda x, pos=i: extract_features(x, pos))
    
    results_df.drop(columns=["Top 5 features influencing this probability"], inplace=True)
    
    table_id = f"{project_id}.{dataset_id}.prediction-results-top"
    job = client.load_table_from_dataframe(results_df, table_id)
    job.result()
    
    return jsonify(results)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
