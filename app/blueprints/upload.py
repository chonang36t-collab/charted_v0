from flask import Blueprint, request, jsonify, current_app
import os
from werkzeug.utils import secure_filename
from app.utils.data_loader import dbDataLoader

upload_bp = Blueprint('upload', __name__)

ALLOWED_EXTENSIONS = {'xlsx', 'xls'}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@upload_bp.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'}), 400
    
    file = request.files['file']
    
    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400
    
    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        
        # Save file temporarily
        upload_folder = os.path.join(current_app.instance_path, 'uploads')
        os.makedirs(upload_folder, exist_ok=True)
        file_path = os.path.join(upload_folder, filename)
        file.save(file_path)
        
        try:
            # Load data into database
            loader = dbDataLoader()
            success = loader.load_excel_data(file_path)
            
            # Clean up temporary file
            os.remove(file_path)
            
            if success:
                return jsonify({'message': 'File successfully uploaded and processed'}), 200
            else:
                return jsonify({'error': 'Failed to process file'}), 500
                
        except Exception as e:
            # Clean up temporary file on error
            if os.path.exists(file_path):
                os.remove(file_path)
            return jsonify({'error': f'Error processing file: {str(e)}'}), 500
    
    return jsonify({'error': 'Invalid file type'}), 400