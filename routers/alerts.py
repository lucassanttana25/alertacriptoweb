from fastapi import APIRouter, Depends, HTTPException, status, Response
from typing import List
from datetime import datetime
from bson import ObjectId

import models
import security
from database import db

router = APIRouter()

@router.post("", status_code=201, response_model=models.AlertaNovoPublic)
async def criar_alerta(data: models.AlertaNovoCreate, current_user: models.UserInDB = Depends(security.get_current_user)):
    """
    Cria um novo alerta para o usuário logado.
    """
    user_id = current_user.id
    asset_ticker = data.assetTicker.upper()
    
    if data.tipo not in ["compra", "venda"]:
        raise HTTPException(status_code=400, detail="O tipo deve ser 'compra' ou 'venda'.")

    alert_document = {
        'userId': user_id,
        'assetTicker': asset_ticker,
        'preco_alvo': data.preco_alvo,
        'ativo': True,
        'tipo': data.tipo,
        'createdAt': datetime.utcnow()
    }

    result = await db.alerts.insert_one(alert_document)
    created_doc = await db.alerts.find_one({"_id": result.inserted_id})

    # Constrói a resposta manualmente para garantir que o ID seja uma string.
    return {
        "id": str(created_doc["_id"]),
        "userId": str(created_doc["userId"]),
        "assetTicker": created_doc["assetTicker"],
        "preco_alvo": created_doc["preco_alvo"],
        "tipo": created_doc["tipo"],
        "ativo": created_doc["ativo"],
    }

@router.get("", response_model=List[models.AlertaNovoPublic])
async def ler_alertas_do_usuario(current_user: models.UserInDB = Depends(security.get_current_user)):
    user_id = current_user.id
    alertas_cursor = db.alerts.find({'userId': user_id})
    alertas_db = await alertas_cursor.to_list(length=None)
    
    # Constrói a lista de resposta manualmente para garantir que os IDs sejam strings.
    response_list = []
    for doc in alertas_db:
        try:
            response_list.append({
                "id": str(doc["_id"]),
                "userId": str(doc["userId"]),
                "assetTicker": doc["assetTicker"],
                "preco_alvo": doc["preco_alvo"],
                "tipo": doc["tipo"],
                "ativo": doc["ativo"],
            })
        except KeyError:
            continue
            
    return response_list

@router.delete("/{alert_id}", status_code=status.HTTP_204_NO_CONTENT)
async def deletar_alerta(alert_id: str, current_user: models.UserInDB = Depends(security.get_current_user)):
    user_id = current_user.id
    if not ObjectId.is_valid(alert_id):
        raise HTTPException(status_code=400, detail="ID do alerta inválido.")
    
    result = await db.alerts.delete_one(
        {'_id': ObjectId(alert_id), 'userId': user_id}
    )

    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Alerta não encontrado ou não pertence ao usuário.")
    
    return Response(status_code=status.HTTP_204_NO_CONTENT)

@router.post("/registrar-dispositivo", status_code=201, tags=["Dispositivos"])
async def registrar_dispositivo(data: models.Dispositivo, current_user: models.UserInDB = Depends(security.get_current_user)):
    user_id = current_user.id
    await db.dispositivos.update_one(
        {'token': data.token},
        {'$set': {'userId': user_id, 'registrado_em': datetime.utcnow()}},
        upsert=True
    )
    return {"mensagem": "Dispositivo registrado com sucesso."}
