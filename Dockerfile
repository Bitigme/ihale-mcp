FROM python:3.11-slim

# Sisteme curl paketini kuruyoruz (uv indirmek için gerekli)
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Çalışma dizinini /app olarak ayarlıyoruz
WORKDIR /app

# uv paket yöneticisini kuruyoruz
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
# uv'yi PATH'e ekliyoruz ki her yerden erişilebilsin
ENV PATH="/root/.local/bin:${PATH}"

# Proje dosyalarının tamamını konteynerin içine kopyalıyoruz
COPY . .

# uv kullanarak projenin bağımlılıklarını kuruyoruz
RUN uv pip install --system .

# Konteyner çalıştığında çalıştırılacak varsayılan komut
CMD ["ihale-mcp"] 