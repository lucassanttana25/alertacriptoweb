from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm

import models
import security
from database import db

# Esta é a linha importante. A variável DEVE se chamar 'router'.
router = APIRouter()

@router.post("/register", response_model=models.UserPublic, status_code=status.HTTP_201_CREATED, summary="Registrar um novo usuário")
async def register_user(user: models.UserCreate):
    existing_user = await db.users.find_one({"email": user.email})
    if existing_user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Este e-mail já está cadastrado.",
        )
    
    hashed_password = security.get_password_hash(user.password)
    user_data = user.model_dump()
    user_data.pop("password")
    user_data["hashed_password"] = hashed_password
    user_data["createdAt"] = datetime.utcnow()

    new_user = await db.users.insert_one(user_data)
    created_user = await db.users.find_one({"_id": new_user.inserted_id})
    
    return models.UserPublic.model_validate(created_user)

@router.post("/token", response_model=models.Token, summary="Autenticar e obter um token JWT")
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
    user = await security.get_user(email=form_data.username)
    if not user or not security.verify_password(form_data.password, user.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="E-mail ou senha incorretos",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    access_token = security.create_access_token(data={"sub": user.email})
    
    return {"access_token": access_token, "token_type": "bearer"}
