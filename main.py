from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
# 1. Adiciona a importação do novo roteador
from routers import authentication, alerts, password_recovery, market_data
import models
import security
from database import db
from startup import create_indexes
from routers import authentication, alerts, password_recovery, market_data, reports

# Inicialização da aplicação FastAPI
app = FastAPI(
    title="API de Alertas de Ativos B3",
    description="API para criar e gerenciar alertas de preços de ativos da B3.",
    version="1.0.0"
)

# --- Evento de Startup ---
@app.on_event("startup")
async def startup_event():
    """
    Executa tarefas na inicialização da API.
    """
    await create_indexes()

# --- CONFIGURAÇÃO DO CORS ---
origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
# --- FIM DA CONFIGURAÇÃO DO CORS ---


# Inclui os roteadores na aplicação principal
app.include_router(authentication.router, prefix="/auth", tags=["Autenticação"])
app.include_router(alerts.router, prefix="/alerts", tags=["Alertas"])
app.include_router(password_recovery.router, prefix="/password", tags=["Recuperação de Senha"])
# 2. Adiciona a nova rota de dados de mercado
app.include_router(market_data.router, prefix="/market", tags=["Dados de Mercado"])
# 3. Adiciona o novo roteador de relatórios
app.include_router(reports.router, prefix="/reports", tags=["Relatórios"])


@app.get("/", summary="Rota raiz da API", tags=["Status"])
async def root():
    """Verifica o status da API."""
    return {"status": "ok", "message": "Bem-vindo à API de Alertas de Ativos da B3"}

@app.get("/users/me", response_model=models.UserPublic, summary="Obter informações do usuário logado", tags=["Usuários"])
async def read_users_me(current_user: models.UserInDB = Depends(security.get_current_user)):
    """Retorna as informações do usuário que está autenticado."""
    return current_user
