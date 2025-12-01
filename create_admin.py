from app import create_app, db
from app.models import User

def create_admin_user():
    app = create_app()
    with app.app_context():
        # Check if admin already exists
        admin = User.query.filter_by(username='admin').first()
        if admin:
            print("Admin user already exists")
            return
        
        # Create new admin user
        admin = User(
            username='admin',
            email='chonang.rai@36t.com',
            role='admin'
        )
        admin.set_password('admin36t')  # You should change this in production!
        
        db.session.add(admin)
        db.session.commit()
        print("Admin user created successfully!")

if __name__ == '__main__':
    create_admin_user()