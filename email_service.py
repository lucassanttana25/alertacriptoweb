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
            <p>Você solicitou a redefinição da sua senha. Clique no link abaixo para criar uma nova senha:</p>
            <a href="{reset_link}" target="_blank">Redefinir minha senha</a>
            <p>Se você não solicitou isso, por favor, ignore este e-mail.</p>
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
    print(f"E-mail de redefinição enviado para {recipient_email}.")

# --- NOVA FUNÇÃO ---
async def send_alert_triggered_email(recipient_email: str, alert_details: dict):
    """
    Envia um e-mail de notificação quando um alerta é disparado.
    """
    ticker = alert_details['ticker']
    tipo = alert_details['tipo']
    target_price = alert_details['target_price']
    current_price = alert_details['current_price']
    
    subject = f"Alerta de Preço Disparado para {ticker}!"
    
    # Formata a mensagem do e-mail
    if tipo == 'compra':
        condition_text = f"caiu abaixo de seu alvo de R$ {target_price:,.2f}"
    else: # venda
        condition_text = f"ultrapassou seu alvo de R$ {target_price:,.2f}"

    html_content = f"""
    <html>
        <body>
            <h2>Alerta de Preço Disparado!</h2>
            <p>Olá,</p>
            <p>Seu alerta de <strong>{tipo}</strong> para o ativo <strong>{ticker}</strong> foi disparado.</p>
            <p>O preço do ativo {condition_text}, atingindo <strong>R$ {current_price:,.2f}</strong>.</p>
            <br>
            <p>Obrigado por usar nosso serviço!</p>
            <p>Equipe Alerta de Ativos B3</p>
        </body>
    </html>
    """

    message = MessageSchema(
        subject=subject,
        recipients=[recipient_email],
        body=html_content,
        subtype="html"
    )

    fm = FastMail(conf)
    await fm.send_message(message)
    print(f"E-mail de ALERTA DISPARADO enviado para {recipient_email} sobre {ticker}.")

