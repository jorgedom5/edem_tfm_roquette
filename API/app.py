from flask import Flask, request, jsonify
from flasgger import Swagger
from google.cloud import storage

app = Flask(__name__)
swagger = Swagger(app)

BUCKET_NAME = 'bronze_zone_roquette'

@app.route('/upload_csv', methods=['POST'])
def upload_csv():
    """
    Sube un archivo CSV al bucket de Google Cloud Storage.

    ---
    tags:
      - File Upload
    parameters:
      - name: file
        in: formData
        type: file
        required: true
        description: El archivo CSV que se va a subir.
    responses:
      200:
        description: Archivo subido exitosamente.
        schema:
          type: object
          properties:
            message:
              type: string
              description: Mensaje de éxito.
      400:
        description: No se encontró ningún archivo para subir.
        schema:
          type: object
          properties:
            error:
              type: string
              description: Mensaje de error.
    """
    file = request.files['file']
    if not file:
        return jsonify({'error': 'No file uploaded'}), 400

    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(file.filename)
    blob.upload_from_file(file.stream)

    return jsonify({'message': 'File uploaded successfully'}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
