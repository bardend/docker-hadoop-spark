How to use
```bash
docker pull bardend123/db-bienes-servicios:v1

# 2. Ejecutar el contenedor
docker run -d \
  --name db-bienes-servicios \
  -p 5432:5432 \
  bardend123/db-bienes-servicios:v1

```