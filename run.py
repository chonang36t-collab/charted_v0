"""CLI runner for Sales Insight.

Usage examples (Windows-friendly):
  python run.py init-db
  python run.py create-admin --username admin --password secret --role admin
  python run.py run --host 0.0.0.0 --port 5000
"""
from __future__ import annotations

import argparse
import getpass
import os

from app import create_app, db
from app.models import User


app = create_app()


def cmd_init_db():
    with app.app_context():
        db.create_all()
        print("Database initialized.")


def cmd_create_admin(username: str | None, password: str | None, role: str):
    with app.app_context():
        if not username:
            username = input("Username: ")
        if not password:
            password = getpass.getpass("Password: ")
        if role not in {"admin", "viewer"}:
            role = "admin"
        if User.query.filter_by(username=username).first():
            print("User already exists.")
            return
        user = User(username=username, role=role)
        user.set_password(password)
        db.session.add(user)
        db.session.commit()
        print(f"Created {role} user '{username}'.")


def cmd_run(host: str, port: int):
    app.run(host=host, port=port, debug=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sales Insight CLI")
    sub = parser.add_subparsers(dest="command")

    p_init = sub.add_parser("init-db")

    p_admin = sub.add_parser("create-admin")
    p_admin.add_argument("--username")
    p_admin.add_argument("--password")
    p_admin.add_argument("--role", default="admin")

    p_run = sub.add_parser("run")
    p_run.add_argument("--host", default="0.0.0.0")
    p_run.add_argument("--port", type=int, default=5000)

    args = parser.parse_args()
    if args.command == "init-db":
        cmd_init_db()
    elif args.command == "create-admin":
        cmd_create_admin(args.username, args.password, args.role)
    elif args.command == "run":
        cmd_run(args.host, args.port)
    else:
        parser.print_help()
