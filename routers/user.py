from fastapi import APIRouter, Depends, HTTPException, status
from typing import List

import models
import security
from database import db

router = APIRouter()

@router.get("/me", response_model=models.UserPublic, summary="Obter informações do utilizador logado")
async def read_users_me(current_user: models.UserInDB = Depends(security.get_current_user)):
    """
    Retorna as informações do utilizador que está autenticado.
    """
    return current_user

@router.put("/me/risk-profile", response_model=models.UserPublic, summary="Atualizar o perfil de risco do utilizador")
async def update_risk_profile(
    profile_data: models.RiskProfileUpdate,
    current_user: models.UserInDB = Depends(security.get_current_user)
):
    """
    Atualiza o perfil de risco (conservador, moderado, arrojado) do utilizador logado.
    """
    user_id = current_user.id
    
    # Atualiza o documento do utilizador no MongoDB
    await db.users.update_one(
        {"_id": user_id},
        {"$set": {"risk_profile": profile_data.risk_profile.value}}
    )
    
    # Busca o documento atualizado para retornar
    updated_user_doc = await db.users.find_one({"_id": user_id})
    
    if updated_user_doc is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Utilizador não encontrado após a atualização.")
        
    return updated_user_doc

