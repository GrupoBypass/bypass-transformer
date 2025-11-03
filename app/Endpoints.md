## 🧩 URL Base

```
http://<host>:5000
```

---

## 🚀 Endpoints

### 🧪 Teste de Conectividade

**`POST /process-test`**

Endpoint simples para testar a comunicação e verificar o payload enviado.

#### Corpo da Requisição

```json
{
  "key": "chave-teste",
  "message": "olá mundo"
}
```

#### Resposta

```json
{
  "status": "ok",
  "payload": {
    "key": "chave-teste",
    "message": "olá mundo"
  }
}
```

---

### 📏 Sensor TOF

**`POST /process-tof`**

Processa dados do sensor **Time of Flight (TOF)**.

#### Corpo da Requisição

```json
{
  "key": "caminho/para/arquivo_tof.csv"
}
```

#### Fluxo

1. Faz o download do arquivo especificado no bucket **S3_RAW**.
2. Executa o método `TofTransformer.main()` no arquivo baixado.
3. Envia o resultado processado de volta para o S3.

#### Resposta

```json
{
  "status": "ok",
  "message": "Processamento concluído"
}
```

---

### 🌡️ Sensor DHT11

**`POST /process-dht11`**

Processa dados do sensor **DHT11** (temperatura e umidade).

#### Corpo da Requisição

```json
{
  "key": "caminho/para/arquivo_dht11.csv"
}
```

#### Resposta

```json
{
  "status": "ok",
  "message": "Processamento concluído"
}
```

---

### 🌬️ Sensor DPS

**`POST /process-dps`**

Processa dados do sensor **DPS** (pressão).

#### Corpo da Requisição

```json
{
  "key": "caminho/para/arquivo_dps.csv"
}
```

#### Resposta

```json
{
  "status": "ok",
  "message": "Processamento concluído"
}
```

---

### ❤️ Sensor Omron

**`POST /process-omron`**

Processa dados do sensor **Omron** (sinais vitais).

#### Corpo da Requisição

```json
{
  "key": "caminho/para/arquivo_omron.csv"
}
```

#### Resposta

```json
{
  "status": "ok",
  "message": "Processamento concluído"
}
```

---

### 🎵 Sensor Piezo

**`POST /process-piezo`**

Processa dados do sensor **Piezoelétrico** (vibração, toque, pressão, etc).

#### Corpo da Requisição

```json
{
  "key": "caminho/para/arquivo_piezo.csv"
}
```

#### Resposta

```json
{
  "status": "ok",
  "message": "Processamento concluído"
}
```

---

### 💡 Sensor Óptico

**`POST /process-optical`**

Processa dados do sensor **Óptico**.

#### Corpo da Requisição

```json
{
  "key": "caminho/para/arquivo_optical.csv"
}
```

#### Resposta

```json
{
  "status": "ok",
  "message": "Processamento concluído"
}
```

---

## ⚙️ Variáveis de Ambiente

As seguintes variáveis são necessárias para execução da API:

| Variável                   | Descrição                             |
| -------------------------- | ------------------------------------- |
| `S3_RAW`                   | Nome do bucket S3 com os dados brutos |
| `AWS_ACCESS_KEY_ID`        | Chave de acesso da AWS                |
| `AWS_SECRET_ACCESS_KEY_ID` | Chave secreta da AWS                  |
| `AWS_SESSION_TOKEN`        | Token de sessão da AWS (se aplicável) |

---

## 🗃️ Caminhos Locais Temporários

Os arquivos baixados do S3 são armazenados temporariamente em `/tmp` durante o processamento:

| Sensor  | Caminho Local            |
| ------- | ------------------------ |
| TOF     | `/tmp/input_tof.csv`     |
| DPS     | `/tmp/input_dps.csv`     |
| DHT11   | `/tmp/input_dht11.csv`   |
| OMRON   | `/tmp/input_omron.csv`   |
| PIEZO   | `/tmp/input_piezo.csv`   |
| OPTICAL | `/tmp/input_optical.csv` |

---

## 🧠 Logs

Os logs são impressos no `stdout` com o seguinte formato:

```
2025-11-03 10:42:15 [INFO] Arquivo baixado localmente: /tmp/input_tof.csv
```

Os logs incluem:

* Requisições recebidas
* Etapas de download/upload no S3
* Execução dos transformadores
* Erros e exceções com stack trace

---

## 🧩 Estrutura Interna

Cada sensor possui um módulo transformador próprio em `modules/sensor/`, por exemplo:

```
modules/
└── sensor/
    ├── tof_transformer.py
    ├── dht11_transformer.py
    ├── omron_transformer.py
    ├── dps_transformer.py
    ├── piezo_transformer.py
    └── optical_transformer.py
```

Cada transformador deve implementar o método:

```python
def main(local_path: str, s3_client, key: str):
    ...
```

---

Quer que eu adicione uma seção de **Quickstart (Como rodar localmente ou com Docker)** no final do README para completá-lo?
Isso deixaria o arquivo pronto para deploy e colaboração.
