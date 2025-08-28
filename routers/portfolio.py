from fastapi import APIRouter, Depends, HTTPException, status, Response
from typing import List
from datetime import datetime
from bson import ObjectId

import models
import security
from database import db

router = APIRouter()

@router.post("", response_model=models.PositionPublic, status_code=status.HTTP_201_CREATED)
async def add_position(position: models.PositionCreate, current_user: models.UserInDB = Depends(security.get_current_user)):
    """
    Adiciona uma nova posição de ativo ao portfólio do utilizador.
    """
    position_doc = position.model_dump()
    position_doc['userId'] = current_user.id
    position_doc['createdAt'] = datetime.utcnow()

    result = await db.positions.insert_one(position_doc)
    created_doc = await db.positions.find_one({"_id": result.inserted_id})
    
    # Constrói a resposta manualmente para garantir que o ID seja uma string.
    return {
        "id": str(created_doc["_id"]),
        "userId": str(created_doc["userId"]),
        "assetTicker": created_doc["assetTicker"],
        "quantidade": created_doc["quantidade"],
        "preco_compra": created_doc["preco_compra"],
        "createdAt": created_doc["createdAt"],
    }

@router.get("", response_model=List[models.PositionPublic])
async def get_portfolio(current_user: models.UserInDB = Depends(security.get_current_user)):
    """
    Retorna todas as posições do portfólio do utilizador logado.
    """
    positions_cursor = db.positions.find({'userId': current_user.id})
    positions = await positions_cursor.to_list(length=None)
    
    # Constrói a lista de resposta manualmente para garantir que os IDs sejam strings.
    response_list = []
    for doc in positions:
        response_list.append({
            "id": str(doc["_id"]),
            "userId": str(doc["userId"]),
            "assetTicker": doc["assetTicker"],
            "quantidade": doc["quantidade"],
            "preco_compra": doc["preco_compra"],
            "createdAt": doc["createdAt"],
        })
    return response_list

@router.delete("/{position_id}", status_code=status.HTTP_204_NO_CONTENT)
async def remove_position(position_id: str, current_user: models.UserInDB = Depends(security.get_current_user)):
    """
    Remove uma posição específica do portfólio pelo seu ID.
    """
    if not ObjectId.is_valid(position_id):
        raise HTTPException(status_code=400, detail="ID da posição inválido.")
    
    result = await db.positions.delete_one(
        {'_id': ObjectId(position_id), 'userId': current_user.id}
    )

    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Posição não encontrada ou não pertence ao utilizador.")
    
    return Response(status_code=status.HTTP_204_NO_CONTENT)
