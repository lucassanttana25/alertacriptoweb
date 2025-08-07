import os
from fastapi_mail import FastMail, MessageSchema, ConnectionConfig
from dotenv import load_dotenv

load_dotenv()

# Configuração para o serviço de e-mail usando variáveis de ambiente
conf = ConnectionConfig(
    MAIL_USERNAME = os.getenv("MAIL_USERNAME"),
    MAIL_PASSWORD = os.getenv("MAIL_PASSWORD"),
    MAIL_FROM = os.getenv("MAIL_FROM"),
    MAIL_PORT = int(os.getenv("MAIL_PORT")),
    MAIL_SERVER = os.getenv("MAIL_SERVER"),
    MAIL_STARTTLS = os.getenv("MAIL_STARTTLS", "True").lower() == "true",
    MAIL_SSL_TLS = os.getenv("MAIL_SSL_TLS", "False").lower() == "true",
    USE_CREDENTIALS = True,
    VALIDATE_CERTS = True
)

async def send_password_reset_email(recipient_email: str, reset_link: str):
    """
    Envia um e-mail de redefinição de senha para o usuário.
    """
    html_content = f"""
    <html>
        <body>
            <h2>Redefinição de Senha</h2>
            <p>Olá,</p>
            <p>Você solicitou a redefinição da sua senha. Por favor, clique no link abaixo para criar uma nova senha:</p>
            <a href="{reset_link}" target="_blank">Redefinir minha senha</a>
            <p>Se você não solicitou isso, por favor, ignore este e-mail.</p>
            <br>
            <p>Obrigado,</p>
            <p>Equipe Alerta de Ativos B3</p>
        </body>
    </html>
    """

    message = MessageSchema(
        subject="Redefinição de Senha - Alerta de Ativos B3",
        recipients=[recipient_email],
        body=html_content,
        subtype="html"
    )

    fm = FastMail(conf)
    await fm.send_message(message)
    print(f"E-mail de redefinição enviado para {recipient_email}. Link: {reset_link}")

