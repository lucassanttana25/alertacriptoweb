from fastapi import APIRouter
from typing import List
from pydantic import BaseModel

from database import db

router = APIRouter()

# Modelo de resposta para o nosso novo endpoint
class TopAsset(BaseModel):
    ativo: str
    total_alertas: int

@router.get("/top-assets", response_model=List[TopAsset], tags=["Relatórios"])
async def get_top_monitored_assets():
    """
    Retorna um ranking dos 5 ativos com mais alertas ativos no sistema.
    """
    pipeline = [
        # 1. Filtra apenas os alertas ativos
        {"$match": {"ativo": True}},
        
        # 2. Agrupa por ticker e conta o número de alertas
        {"$group": {
            "_id": "$assetTicker",
            "total_alertas": {"$sum": 1}
        }},
        
        # 3. Ordena do maior para o menor
        {"$sort": {"total_alertas": -1}},
        
        # 4. Limita aos 5 primeiros resultados
        {"$limit": 5},
        
        # 5. Formata o documento de saída para ser mais amigável
        {"$project": {
            "_id": 0,
            "ativo": "$_id",
            "total_alertas": 1
        }}
    ]

    # Executa a agregação no banco de dados
    cursor = db.alerts.aggregate(pipeline)
    results = await cursor.to_list(length=5)
    
    return results
