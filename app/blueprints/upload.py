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
        
        def generate_response():
            loader = dbDataLoader()
            import json
            try:
                # Iterate through the generator
                for status_update in loader.load_excel_data(file_path):
                    yield json.dumps(status_update) + '\n'
            except Exception as e:
                # This catches any error in the generator itself if not handled there
                yield json.dumps({"status": "error", "message": f"Upload error: {str(e)}"}) + '\n'
            finally:
                # Clean up file after processing matches
                if os.path.exists(file_path):
                    try:
                        os.remove(file_path)
                    except:
                        pass

        from flask import Response, stream_with_context
        return Response(stream_with_context(generate_response()), mimetype='application/json')
    
    return jsonify({'error': 'Invalid file type'}), 400