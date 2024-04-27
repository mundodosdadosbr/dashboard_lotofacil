# Use uma imagem base oficial do Python
FROM mundodosdadosbr/dashboards_base:1.0.1

# Define o diretório de trabalho no contêiner
WORKDIR /app

# Copia os arquivos de código do diretório analitica_dash para o contêiner
COPY . .

# Expõe a porta que o app usa (altere para a porta correta do seu app)
EXPOSE 8052

CMD ["python", "lotofacil.py"]