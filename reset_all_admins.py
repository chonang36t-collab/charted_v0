from app import create_app, db
from app.models import User

def reset_all_admins_2fa():
    app = create_app()
    with app.app_context():
        admins = User.query.filter_by(role='admin').all()
        
        for user in admins:
            print(f"Resetting 2FA for admin: {user.username}...")
            user.two_factor_enabled = True
            user.two_factor_setup_complete = False
            user.otp_secret = None
        
        db.session.commit()
        print("2FA reset complete for ALL admins.")

if __name__ == "__main__":
    reset_all_admins_2fa()
