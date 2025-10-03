from fastapi import APIRouter, Depends
from typing import List
from pydantic import BaseModel

import security
from database import redis_client, safe_redis_xrevrange, ensure_redis_streams
from models import UserInDB

router = APIRouter()

class TriggeredAlert(BaseModel):
    id: str
    data: dict

@router.get("/triggered-alerts", response_model=List[TriggeredAlert], tags=["Histórico"])
async def get_triggered_alerts_for_user(current_user: UserInDB = Depends(security.get_current_user)):
    """
    Lê o stream de alertas disparados do Redis e retorna apenas os eventos
    que pertencem ao utilizador atualmente logado.
    Reconstrói o stream se necessário.
    """
    user_id = str(current_user.id)
    user_alerts = []

    # Garante que o stream existe
    await ensure_redis_streams()
    
    # Lê o stream de forma segura
    stream_events = safe_redis_xrevrange('alertas_disparados')
    
    if not stream_events:
        print(f"⚠️  Stream 'alertas_disparados' vazio para usuário {user_id}")
        return []
    
    # Filtra eventos do usuário
    for event_id, event_data in stream_events:
        # Filtra os eventos para incluir apenas os do utilizador logado
        if event_data.get('userId') == user_id:
            # Ignora eventos de inicialização do sistema
            if event_data.get('type') != 'system_init':
                user_alerts.append(TriggeredAlert(id=event_id, data=event_data))
    
    print(f"📊 Encontrados {len(user_alerts)} alertas disparados para usuário {user_id}")
    return user_alerts
