from fastapi import APIRouter, Depends, HTTPException, status, Response
from typing import List
from datetime import datetime
from bson import ObjectId
import json

import models
import security
# Importa funções seguras do Redis da nossa configuração
from database import db, redis_client, safe_redis_delete, safe_redis_get, safe_redis_set, rebuild_user_cache_from_mongo

router = APIRouter()

@router.post("", status_code=201, response_model=models.AlertaNovoPublic)
async def criar_alerta(data: models.AlertaNovoCreate, current_user: models.UserInDB = Depends(security.get_current_user)):
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

    # --- CACHE: Invalidação ---
    # Após criar um novo alerta, removemos o cache antigo para forçar uma nova busca no banco e substituir.
    cache_key = f"alerts:{str(user_id)}"
    if safe_redis_delete(cache_key):
        print(f"Cache invalidado para o usuário: {user_id}")
    else:
        print(f"⚠️  Não foi possível invalidar cache para o usuário: {user_id}")
    
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
    user_id = str(current_user.id)
    cache_key = f"alerts:{user_id}"

    # --- CACHE: Leitura com reconstrução automática ---
    cache_key = f"alerts:{user_id}"
    cached_alerts = safe_redis_get(cache_key)
    
    if cached_alerts:
        try:
            print(f"✅ Cache HIT para o usuário: {user_id}")
            return json.loads(cached_alerts)
        except json.JSONDecodeError as e:
            print(f"⚠️  Cache corrompido para usuário {user_id}: {e}")
            # Cache corrompido, remove e continua para buscar no MongoDB
            safe_redis_delete(cache_key)

    print(f"🔄 Cache MISS para o usuário: {user_id}. Buscando no MongoDB.")
    alertas_cursor = db.alerts.find({'userId': current_user.id})
    alertas_db = await alertas_cursor.to_list(length=None)
    
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
    
    # --- CACHE: Escrita com verificação ---
    alerts_json = json.dumps(response_list, default=str)
    if safe_redis_set(cache_key, alerts_json, ex=3600):  # Expira em 1 hora
        print(f"✅ Cache populado para o usuário: {user_id}")
    else:
        print(f"⚠️  Não foi possível salvar cache para o usuário: {user_id}")
            
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
    
    # --- LÓGICA DE CACHE: Invalidação ---
    cache_key = f"alerts:{str(user_id)}"
    if safe_redis_delete(cache_key):
        print(f"✅ Cache invalidado para o usuário: {user_id}")
    else:
        print(f"⚠️  Não foi possível invalidar cache para o usuário: {user_id}")

    return Response(status_code=status.HTTP_204_NO_CONTENT)