# EC2 Ubuntu PySpark Processor

## Passos rápidos:

1. Crie a instância EC2 Ubuntu 22.04 (t3.medium recomendado).
2. Copie este bundle para a instância e extraia em `/home/ubuntu/app`.
3. (Opcional) Use `user-data.sh` no campo de User Data ao criar a instância.
4. Configure credenciais AWS em `~/.aws/credentials` se for usar S3/DynamoDB.
5. Ative o serviço Flask:
   ```bash
   sudo mv /home/ubuntu/app/flask-app.service /etc/systemd/system/
   sudo systemctl daemon-reload
   sudo systemctl enable --now flask-app.service
   sudo systemctl status flask-app.service
   ```
6. Teste upload:
   ```bash
   curl -F "file=@./meu_teste.csv" http://<EC2_IP>:5000/upload
   ```
7. Teste processamento:
   ```bash
   curl -X POST http://<EC2_IP>:5000/process -H "Content-Type: application/json" -d '{"input_path":"/home/ubuntu/uploads/meu_teste.csv"}'
   ```
