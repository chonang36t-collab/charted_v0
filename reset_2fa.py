from app import create_app, db
from app.models import User

def reset_admin_2fa():
    app = create_app()
    with app.app_context():
        # Reset for 'Jehan' as he is likely the admin attempting login
        # Also reset 'testadmin' just in case
        users_to_reset = ['Jehan', 'testadmin']
        
        for username in users_to_reset:
            user = User.query.filter_by(username=username).first()
            if user:
                print(f"Resetting 2FA for {username}...")
                user.two_factor_enabled = True
                user.two_factor_setup_complete = False
                user.otp_secret = None
                db.session.commit()
                print(f"2FA reset complete for {username}. They should now see the setup screen.")
            else:
                print(f"User {username} not found.")

if __name__ == "__main__":
    reset_admin_2fa()
