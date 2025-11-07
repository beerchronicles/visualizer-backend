from os import getenv

gral_base_url = getenv("GRAL_BASE_URL") or "http://localhost:5000"
gral_path = getenv("GRAL_PATH") or "D:\Minecraft Developement\GRAL"

postgres_host = getenv("POSTGRES_HOST") or "localhost"
postgres_user = getenv("POSTGRES_USER") or "postgres"
postgres_password = getenv("POSTGRES_PASSWORD") or "postgres"
postgres_port = getenv("POSTGRES_PORT") or "5432"
postgres_database = getenv("POSTGRES_DB") or "visualizer"

postgres_url = f"postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_database}"