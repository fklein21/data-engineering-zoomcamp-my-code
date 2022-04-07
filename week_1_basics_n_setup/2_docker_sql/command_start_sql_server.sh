docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny-taxi" \
  -v "$(pwd)/ny_taxi-volume:/var/lib/postgresql/data" \
  -p 5432:5432 \
  --network=pg-network \
  --name pg-database \
  postgres:13