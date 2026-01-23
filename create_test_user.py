from app import create_app, db
from app.models import User

def create_test_user():
    app = create_app()
    with app.app_context():
        # Check if testadmin already exists
        user = User.query.filter_by(username='testadmin').first()
        if user:
            print("Test user already exists, resetting password...")
        else:
            user = User(
                username='testadmin',
                email='test@example.com',
                role='admin'
            )
            db.session.add(user)
        
        user.set_password('password123')
        # Ensure 2FA is enabled but not set up
        user.two_factor_enabled = True
        user.two_factor_setup_complete = False
        user.otp_secret = None
        
        db.session.commit()
        print("Test admin user 'testadmin' created/reset with password 'password123'")

if __name__ == "__main__":
    create_test_user()
