from flask import Flask, request, jsonify
from google.cloud import storage
import os

app = Flask(__name__)

BUCKET_NAME = 'bronze_zone_roquette'

@app.route('/upload_csv', methods=['POST'])
def upload_csv():
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