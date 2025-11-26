FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .

# Instala las dependencias
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copia el script de importación
COPY import_csv.py .

# Comando por defecto
CMD ["python", "import_csv.py"]