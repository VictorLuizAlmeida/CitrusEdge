# Imagem base leve, já com Python 3.9
FROM python:3.9-slim

# Evita prompts interativos
ENV DEBIAN_FRONTEND=noninteractive

# Instala dependências de sistema necessárias (CatBoost, numpy, pandas, pg8000)
RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    g++ \
    cmake \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Define diretório de trabalho
WORKDIR /app

# Copia requirements e instala dependências Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia código e modelo
COPY app/ ./app/
COPY cb_v0.pkl .

# Comando de entrada: roda o main.py
CMD ["python", "app/main.py"]
