from app import create_app
import urllib.parse

app = create_app()
print("Registered Routes:")
for rule in app.url_map.iter_rules():
    print(f"{rule.endpoint}: {rule.rule} {rule.methods}")
