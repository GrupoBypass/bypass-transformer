## ⚙️ Pré-requisitos

Antes de começar, você precisará:

* Ter acesso ao **Vocareum AWS Lab** (as credenciais temporárias vêm de lá);
* Ter recebido por e-mail a chave SSH chamada **`bypass-key`** (❗ **não gerar outra**, usar apenas essa);
* Ter um **bucket S3** criado para armazenar o estado remoto do Terraform (`tfstate`).

---

## 🔐 Configurando os **Organization Secrets**

1. Vá até a **organização no GitHub**.
2. Acesse **Settings → Secrets and variables → Actions → Organization secrets**.
3. Altere os seguintes **secrets**:

| Nome do Secret          | Descrição                                                     | Valor a ser inserido                                                                                |
| ----------------------- | ------------------------------------------------------------- | --------------------------------------------------------------------------------------------------- |
| `AWS_ACCESS_KEY_ID`     | Chave de acesso da AWS (Vocareum)                             | Copie do painel do Vocareum                                                                         |
| `AWS_SECRET_ACCESS_KEY` | Chave secreta da AWS (Vocareum)                               | Copie do painel do Vocareum                                                                         |
| `AWS_SESSION_TOKEN`     | Token de sessão temporário (Vocareum)                         | Copie do painel do Vocareum                                                                         |
| `EC2_SSH_KEY`           | **Chave SSH privada** utilizada para acesso às instâncias EC2 | ⚠️ **NÃO MEXER**                                                                                    |
| `STATE_BUCKET_NAME`     | Nome do bucket S3 usado como backend do Terraform             | Crie um bucket na sua conta AWS e coloque o nome aqui                                               |

> 💡 **Importante:**
>
> * As credenciais do Vocareum expiram periodicamente. Se o pipeline falhar, atualize as três variáveis `AWS_*`.
> * O bucket precisa existir antes da execução.
> * A chave `bypass-key` é **compartilhada e obrigatória** — **não altere, não substitua e não gere outra**.
> * Certifique-se de que os secrets estejam **visíveis para os repositórios** que precisam utilizá-los (opção “Repository access”).

---

## 🧰 Executando o pipeline

Após configurar os **organization secrets**:

1. Faça um **push** na `main`, **ou**
2. Vá até a aba **Actions**, selecione o workflow e clique em **"Re-run job"**.

O GitHub Actions fará o resto automaticamente 🎯

---

## 📦 O que o pipeline faz

* Inicializa o Terraform com backend remoto no S3.
* Valida e aplica a infraestrutura definida nos arquivos `.tf`.
* Usa a chave `bypass-key` para permitir acesso SSH à instância EC2.

---

## ✅ Dicas

* Se o workflow falhar por **problemas de credenciais**, verifique se o token do Vocareum ainda é válido.
* Caso o **bucket S3** ainda não exista, crie manualmente antes de reexecutar.
* Você pode ver os logs completos de execução na aba **Actions** do GitHub.
* Se o repositório não estiver conseguindo acessar os secrets, revise as **permissões de acesso do organization secret**.

---

Quer que eu adicione também uma **seção curta mostrando como vincular o secret da organização ao repositório** (com a opção “Repository access”)? Isso ajuda a evitar erro comum em labs compartilhados.
