import pymongo
from pymongo import MongoClient
from faker import Faker
import random
from datetime import datetime, timedelta
from bson.objectid import ObjectId
import os 


MONGO_URI = "mongodb+srv://clusterdd:OjvLFBiRrwW1bxSy@clusterdd.tscsomc.mongodb.net/?retryWrites=true&w=majority&appName=ClusterDD"

DB_NAME = "alertacripto_db"

fake = Faker('pt_BR') 

def limpar_colecoes(db):
    """Apaga todas as coleções do banco para garantir um estado limpo."""
    print("Limpando coleções existentes...")
    for collection_name in db.list_collection_names():
        db.drop_collection(collection_name)
    print("Coleções limpas com sucesso.")

def criar_criptomoedas(db):
    """Cria e insere as criptomoedas base no banco."""
    print("Criando coleção 'criptomoedas'...")
    collection = db.criptomoedas
    
    criptos = [
        {
            "nome": "Bitcoin", "simbolo": "BTC", "logo_url": "https://example.com/logos/btc.png",
            "descricao": "A primeira e mais conhecida criptomoeda descentralizada."
        },
        {
            "nome": "Ethereum", "simbolo": "ETH", "logo_url": "https://example.com/logos/eth.png",
            "descricao": "Uma plataforma global de código aberto para aplicações descentralizadas."
        },
        {
            "nome": "Cardano", "simbolo": "ADA", "logo_url": "https://example.com/logos/ada.png",
            "descricao": "Uma plataforma de blockchain de prova de participação (PoS) com foco em segurança e sustentabilidade."
        },
        {
            "nome": "Solana", "simbolo": "SOL", "logo_url": "https://example.com/logos/sol.png",
            "descricao": "Uma blockchain de alto desempenho projetada para aplicações descentralizadas escaláveis."
        }
    ]
    
    try:
        resultado = collection.insert_many(criptos)
        print(f"{len(resultado.inserted_ids)} criptomoedas inseridas.")
        return resultado.inserted_ids
    except Exception as e:
        print(f"Erro ao inserir criptomoedas: {e}")
        return []

def criar_usuarios(db, quantidade=20):
    """Cria e insere usuários fictícios."""
    print(f"Criando coleção 'usuarios' com {quantidade} usuários...")
    collection = db.usuarios
    usuarios = []
    for _ in range(quantidade):
        nome = fake.name()
        usuarios.append({
            "nome": nome,
            "email": fake.email(domain='criptomail.com'),
            "senha_hash": fake.sha256(),
            "telefone": fake.phone_number(),
            "data_criacao": fake.past_datetime(start_date="-2y", tzinfo=None)
        })
        
    try:
        resultado = collection.insert_many(usuarios)
        print(f"{len(resultado.inserted_ids)} usuários inseridos.")
        return resultado.inserted_ids
    except Exception as e:
        print(f"Erro ao inserir usuários: {e}")
        return []

def criar_historico_precos(db, cripto_ids_map):
    """Gera um histórico de preços fictício para cada criptomoeda."""
    print("Criando coleção 'historico_precos'...")
    collection = db.historico_precos
    historico = []
    
    precos_base = {"BTC": 68000.0, "ETH": 3500.0, "ADA": 0.45, "SOL": 150.0}
    agora = datetime.utcnow()
    
    for cripto_id in cripto_ids_map:
        simbolo = db.criptomoedas.find_one({"_id": cripto_id})['simbolo']
        preco_atual = precos_base[simbolo]
        
        for i in range(240):
            preco_variado = preco_atual * random.uniform(0.995, 1.005)
            timestamp = agora - timedelta(hours=i)
            historico.append({
                "criptomoeda_id": cripto_id,
                "preco_usd": round(preco_variado, 4),
                "timestamp": timestamp
            })
            preco_atual = preco_variado
    
    try:
        resultado = collection.insert_many(historico)
        print(f"{len(resultado.inserted_ids)} registros de preço inseridos.")
    except Exception as e:
        print(f"Erro ao inserir histórico de preços: {e}")

def criar_alertas(db, usuario_ids, cripto_ids_map):
    """Cria alertas fictícios para os usuários."""
    print("Criando coleção 'alertas'...")
    collection = db.alertas
    alertas = []
    
    for _ in range(len(usuario_ids) * 2):
        usuario_id = random.choice(usuario_ids)
        cripto_id = random.choice(cripto_ids_map)
        
        ultimo_preco_doc = db.historico_precos.find_one(
            {"criptomoeda_id": cripto_id},
            sort=[("timestamp", pymongo.DESCENDING)]
        )
        if not ultimo_preco_doc: continue
            
        ultimo_preco = ultimo_preco_doc['preco_usd']
        tipo_condicao = random.choice(["acima_de", "abaixo_de"])
        
        if tipo_condicao == "acima_de":
            valor_gatilho = ultimo_preco * random.uniform(1.05, 1.20)
        else:
            valor_gatilho = ultimo_preco * random.uniform(0.80, 0.95)
            
        alertas.append({
            "usuario_id": usuario_id,
            "criptomoeda_id": cripto_id,
            "tipo_condicao": tipo_condicao,
            "valor_gatilho": round(valor_gatilho, 4),
            "status": random.choice(["ativo", "acionado", "desativado"]),
            "notificacao_enviada": random.choice([True, False]),
            "data_criacao": fake.past_datetime(start_date="-1y", tzinfo=None)
        })
        
    try:
        resultado = collection.insert_many(alertas)
        print(f"{len(resultado.inserted_ids)} alertas inseridos.")
    except Exception as e:
        print(f"Erro ao inserir alertas: {e}")

def main():
    """Função principal para orquestrar a criação e povoamento do banco."""
    client = None
    try:
        client = MongoClient(MONGO_URI)
        client.admin.command('ping')
        print(f"✅ Conexão com o MongoDB Atlas estabelecida com sucesso!")
        
        db = client[DB_NAME]
        
        print(f"Usando o banco de dados: '{DB_NAME}'")
        
        limpar_colecoes(db)
        
        cripto_ids = criar_criptomoedas(db)
        if not cripto_ids: return
        
        usuario_ids = criar_usuarios(db, 30)
        if not usuario_ids: return
        
        criar_historico_precos(db, cripto_ids)
        criar_alertas(db, usuario_ids, cripto_ids)
        
        print("\n--- Processo Finalizado! ---")
        print("Banco de dados 'alertacripto_db' populado com sucesso.")
        
    except pymongo.errors.ConnectionFailure as e:
        print(f"❌ Não foi possível conectar ao MongoDB: {e}")
        print("Verifique sua string de conexão (MONGO_URI), a senha e se seu IP está liberado no MongoDB Atlas.")
    except Exception as e:
        print(f"❌ Ocorreu um erro inesperado: {e}")
    finally:
        if client:
            client.close()
            print("\nConexão com o MongoDB fechada.")

if __name__ == "__main__":
    main()