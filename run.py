"""
Entry point for running the Flask application.
For production deployment with gunicorn: gunicorn run:app
"""
from app import create_app

app = create_app()

if __name__ == '__main__':
    # Development server
    app.run(host='0.0.0.0', port=5000, debug=True)
