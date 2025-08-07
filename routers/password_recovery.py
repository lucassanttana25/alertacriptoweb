import secrets
from datetime import datetime, timedelta
from fastapi import APIRouter, HTTPException, status, Request

import models
import security
from database import db
from email_service import send_password_reset_email

router = APIRouter()

@router.post("/forgot-password", status_code=status.HTTP_200_OK, summary="Solicitar redefinição de senha")
async def forgot_password(request_body: models.PasswordResetRequest, request: Request):
    """
    Inicia o fluxo de recuperação de senha.
    O usuário fornece o e-mail e a API envia um link de redefinição.
    """
    user = await db.users.find_one({"email": request_body.email})
    if not user:
        # Não informamos ao usuário se o e-mail existe ou não por segurança.
        # A resposta é sempre a mesma.
        return {"message": "Se um usuário com este e-mail existir, um link de redefinição será enviado."}

    # Gerar um token seguro e aleatório
    reset_token = secrets.token_urlsafe(32)
    # Definir tempo de expiração (ex: 1 hora)
    expire_date = datetime.utcnow() + timedelta(hours=1)

    # Salvar o token e a data de expiração no banco de dados
    await db.users.update_one(
        {"_id": user["_id"]},
        {"$set": {"reset_token": reset_token, "reset_token_expires": expire_date}}
    )

    # Montar o link de redefinição (a URL do frontend será adicionada depois)
    # Por enquanto, o link aponta para uma rota hipotética no frontend
    base_url = str(request.base_url) # Ex: http://127.0.0.1:8000/
    reset_link = f"{base_url.replace('http', 'http', 1)}reset-password?token={reset_token}" # URL do frontend (ajustar depois)
    
    # Enviar o e-mail
    try:
        await send_password_reset_email(
            recipient_email=user["email"],
            reset_link=reset_link
        )
    except Exception as e:
        print(f"Falha ao enviar e-mail: {e}")
        # Mesmo se o e-mail falhar, não informamos o usuário para evitar vazamento de informações.
        # Idealmente, teríamos um sistema de logs para monitorar isso.
        pass

    return {"message": "Se um usuário com este e-mail existir, um link de redefinição será enviado."}


@router.post("/reset-password", status_code=status.HTTP_200_OK, summary="Redefinir a senha")
async def reset_password(request_body: models.PasswordReset):
    """
    Finaliza o fluxo de recuperação de senha.
    O usuário fornece o token (do link do e-mail) e a nova senha.
    """
    token = request_body.token
    new_password = request_body.new_password

    # Buscar o usuário pelo token e verificar se não expirou
    user = await db.users.find_one({
        "reset_token": token,
        "reset_token_expires": {"$gt": datetime.utcnow()}
    })

    if not user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Token de redefinição inválido ou expirado."
        )

    # Atualizar a senha
    hashed_password = security.get_password_hash(new_password)
    await db.users.update_one(
        {"_id": user["_id"]},
        {
            "$set": {"hashed_password": hashed_password},
            "$unset": {"reset_token": "", "reset_token_expires": ""} # Limpa os campos do token
        }
    )

    return {"message": "Sua senha foi redefinida com sucesso."}

