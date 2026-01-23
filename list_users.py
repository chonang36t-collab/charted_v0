from app import create_app, db
from app.models import User

def list_users():
    app = create_app()
    with app.app_context():
        users = User.query.all()
        print(f"{'Username':<20} {'Role':<10} {'2FA Enabled':<12} {'Setup Complete':<15} {'Secret Set':<10}")
        print("-" * 70)
        for user in users:
            secret_set = "Yes" if user.otp_secret else "No"
            print(f"{user.username:<20} {user.role:<10} {str(user.two_factor_enabled):<12} {str(user.two_factor_setup_complete):<15} {secret_set:<10}")

if __name__ == "__main__":
    list_users()
