from flask import Flask, jsonify
from google.cloud import bigquery
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import joblib
from sklearn.preprocessing import StandardScaler

app = Flask(__name__)

# Configuración de BigQuery
project_id = "banded-setting-428309-q4"
dataset_id = "datos"

# Cargar el modelo y el scaler guardados localmente
model_filename = 'logistic_model.pkl'
scaler_filename = 'scaler_model.pkl'
loaded_model = joblib.load(model_filename)
scaler = joblib.load(scaler_filename)

@app.route('/last_week_data', methods=['GET'])
def get_last_week_data():
    client = bigquery.Client()
    one_week_ago = datetime.now() - timedelta(days=7)
    
    query = f"""
    SELECT
    FORMAT_TIMESTAMP('%Y-%m-%d', Timestamp) AS Day,
    FORMAT_TIMESTAMP('%H', Timestamp) AS Hour,
    CASE
        WHEN FORMAT_TIMESTAMP('%M', Timestamp) BETWEEN '00' AND '07' THEN '00'
        WHEN FORMAT_TIMESTAMP('%M', Timestamp) BETWEEN '08' AND '22' THEN '15'
        WHEN FORMAT_TIMESTAMP('%M', Timestamp) BETWEEN '23' AND '37' THEN '30'
        WHEN FORMAT_TIMESTAMP('%M', Timestamp) BETWEEN '38' AND '52' THEN '45'
        ELSE '00'
    END AS Minute,
    ct.descripcion,
    bd.Value
FROM `banded-setting-428309-q4.datos.bronze-data` bd
LEFT JOIN `banded-setting-428309-q4.datos.col-tag` ct on bd.Tag = ct.tag
WHERE DATE(Timestamp) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 20 DAY) AND CURRENT_DATE()
    """
    
    query_job = client.query(query)
    results = query_job.result()
    
    data = []
    for row in results:
        data.append(dict(row))
    
    # Convertir los resultados a un DataFrame
    df = pd.DataFrame(data)

    df_max_values = df.groupby(["descripcion", "Day", "Hour", "Minute"]).agg({"Value": "max"}).reset_index()

    #Despivotar
    df_max_values['dayhourminute'] = df_max_values['Day'] + ' ' + df_max_values['Hour'] + ':' + df_max_values['Minute']
    df_unpivot = df_max_values.pivot_table(index="dayhourminute", columns="descripcion", values="Value", aggfunc="max").reset_index()
    
    # Preprocesar los datos
    col_drop = ['COT AGUAS ÁCIDAS NUEVO', 'COT AGUAS ÁCIDAS', 'COR TITÁNIC AZÚCARES', 'COT TITÁNIC AZÚCARES NUEVO','dayhourminute']  
    df = df_unpivot.drop(columns=[col for col in col_drop if col in df_unpivot.columns])
    df = df.fillna(0)
    
    X = df  
    X_scaled = scaler.transform(X)

    # Hacer predicciones
    probabilities = loaded_model.predict_proba(X_scaled)[:, 1]
    
    # Obtener las 5 características más influyentes
    coefficients = loaded_model.coef_[0]
    influences = coefficients * X_scaled
    
    results = []
    for i in range(len(probabilities)):
        feature_values = X_scaled[i]
        influence = coefficients * feature_values
        top_5_indices = np.argsort(np.abs(influence))[-5:]
        top_5_features = X.columns[top_5_indices]
        top_5_influences = influence[top_5_indices]
        top_5 = pd.DataFrame({
            'Feature': top_5_features,
            'Influence': top_5_influences
        }).sort_values(by='Influence', ascending=False)
        
        result = {
            "Probability that flag is 1": probabilities[i],
            "Top 5 features influencing this probability": top_5.to_dict(orient='records')
        }
        results.append(result)
    
    return jsonify(results)

if __name__ == '__main__':
    app.run(debug=True)
