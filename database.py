import os
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
import redis
import json
from datetime import datetime

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DATABASE_NAME = os.getenv("DATABASE_NAME")

client = AsyncIOMotorClient(MONGO_URI)
db = client[DATABASE_NAME]

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_USERNAME = os.getenv("REDIS_USERNAME", None) 
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None)

try:
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, username=REDIS_USERNAME, password=REDIS_PASSWORD, db=0, decode_responses=True)
    redis_client.ping()
    print("Conectado ao Redis com sucesso!")
except redis.exceptions.ConnectionError as e:
    print(f"ERRO: Não foi possível conectar ao Redis. Verifique se o servidor está rodando. Erro: {e}")
    redis_client = None

# Funções auxiliares para gerenciar o Redis
def check_redis_health():
    """Verifica se o Redis está funcionando corretamente."""
    if redis_client is None:
        return False
    
    try:
        redis_client.ping()
        return True
    except:
        return False

def redis_key_exists(key):
    """Verifica se uma chave existe no Redis."""
    if not check_redis_health():
        return False
    
    try:
        return redis_client.exists(key) > 0
    except:
        return False

def get_redis_stream_length(stream_name):
    """Retorna o número de eventos em um stream Redis."""
    if not check_redis_health():
        return 0
    
    try:
        return redis_client.xlen(stream_name)
    except:
        return 0

async def ensure_redis_streams():
    """Garante que os streams necessários existem no Redis."""
    if not check_redis_health():
        print("⚠️  Redis não disponível - streams não serão criados")
        return False
    
    try:
        # Verifica se o stream de alertas disparados existe
        if get_redis_stream_length('alertas_disparados') == 0:
            print("🔄 Stream 'alertas_disparados' vazio ou inexistente - criando entrada inicial...")
            
            # Cria uma entrada inicial no stream para inicializá-lo
            redis_client.xadd('alertas_disparados', {
                'type': 'system_init',
                'message': 'Stream inicializado',
                'timestamp': datetime.utcnow().isoformat()
            })
            print("✅ Stream 'alertas_disparados' inicializado")
        
        return True
        
    except Exception as e:
        print(f"❌ Erro ao inicializar streams Redis: {e}")
        return False

async def rebuild_user_cache_from_mongo(user_id):
    """Reconstrói o cache de alertas de um usuário a partir do MongoDB."""
    if not check_redis_health():
        return False
    
    try:
        from bson import ObjectId
        
        # Busca alertas do usuário no MongoDB
        user_object_id = ObjectId(user_id) if isinstance(user_id, str) else user_id
        alertas_cursor = db.alerts.find({'userId': user_object_id})
        alertas_db = await alertas_cursor.to_list(length=None)
        
        # Constrói lista de resposta
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
        
        # Salva no cache Redis
        cache_key = f"alerts:{str(user_id)}"
        alerts_json = json.dumps(response_list, default=str)
        redis_client.set(cache_key, alerts_json, ex=3600)  # Expira em 1 hora
        
        print(f"✅ Cache reconstruído para usuário {user_id}: {len(response_list)} alertas")
        return True
        
    except Exception as e:
        print(f"❌ Erro ao reconstruir cache do usuário {user_id}: {e}")
        return False

def safe_redis_get(key, default=None):
    """Get seguro do Redis com fallback."""
    if not check_redis_health():
        return default
    
    try:
        result = redis_client.get(key)
        return result if result is not None else default
    except Exception as e:
        print(f"Erro ao acessar chave Redis '{key}': {e}")
        return default

def safe_redis_set(key, value, ex=None):
    """Set seguro do Redis com tratamento de erro."""
    if not check_redis_health():
        return False
    
    try:
        redis_client.set(key, value, ex=ex)
        return True
    except Exception as e:
        print(f"Erro ao definir chave Redis '{key}': {e}")
        return False

def safe_redis_delete(key):
    """Delete seguro do Redis com tratamento de erro."""
    if not check_redis_health():
        return False
    
    try:
        redis_client.delete(key)
        return True
    except Exception as e:
        print(f"Erro ao deletar chave Redis '{key}': {e}")
        return False

def safe_redis_xadd(stream, fields):
    """XADD seguro do Redis com tratamento de erro."""
    if not check_redis_health():
        return None
    
    try:
        return redis_client.xadd(stream, fields)
    except Exception as e:
        print(f"Erro ao adicionar ao stream Redis '{stream}': {e}")
        return None

def safe_redis_xrevrange(stream, max_id='+', min_id='-', count=None):
    """XREVRANGE seguro do Redis com tratamento de erro."""
    if not check_redis_health():
        return []
    
    try:
        kwargs = {'max': max_id, 'min': min_id}
        if count is not None:
            kwargs['count'] = count
        return redis_client.xrevrange(stream, **kwargs)
    except Exception as e:
        print(f"Erro ao ler stream Redis '{stream}': {e}")
        return []