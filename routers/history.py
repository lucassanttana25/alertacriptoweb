from fastapi import APIRouter, Depends
from typing import List
from pydantic import BaseModel

import security
from database import redis_client
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
    """
    user_id = str(current_user.id)
    user_alerts = []

    if redis_client:
        try:
            # XREVRANGE lê o stream em ordem inversa (do mais novo para o mais antigo)
            # O '+' e '-' representam o início e o fim do tempo.
            stream_events = redis_client.xrevrange('alertas_disparados', max='+', min='-')
            
            for event_id, event_data in stream_events:
                # Filtra os eventos para incluir apenas os do utilizador logado
                if event_data.get('userId') == user_id:
                    user_alerts.append(TriggeredAlert(id=event_id, data=event_data))
        except Exception as e:
            print(f"Erro ao ler do Redis Stream: {e}")
            # Retorna uma lista vazia em caso de erro
            return []
            
    return user_alerts
