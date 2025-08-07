from database import db
from pymongo import ASCENDING

async def create_indexes():
    """
    Cria os índices no MongoDB para otimizar as consultas.
    Esta função é chamada na inicialização da aplicação.
    """
    print("Verificando e criando índices no MongoDB...")
    
    # --- Índices para a coleção 'users' ---
    await db.users.create_index("email", unique=True)
    
    # --- Índices para a coleção 'alerts' ---
    
    
    await db.alerts.create_index(
        [("userId", ASCENDING), ("assetTicker", ASCENDING)]
    )

    # Índice para o worker encontrar rapidamente todos os alertas ativos.
    await db.alerts.create_index("ativo")

    # --- Índices para a coleção 'dispositivos' ---
    try:
        await db.dispositivos.create_index("token", unique=True)
    except Exception as e:
        print(f"Aviso: Não foi possível criar índice para 'dispositivos' (pode não existir ainda): {e}")

    print("Verificação de índices concluída.")
