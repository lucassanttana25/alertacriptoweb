from datetime import datetime
from typing import Optional, Any
from enum import Enum
from pydantic import BaseModel, EmailStr, Field, field_validator
from bson import ObjectId
from pydantic_core import core_schema

# --- Modelo Auxiliar para ObjectId do Mongo ---
class PyObjectId(ObjectId):
    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler
    ) -> core_schema.CoreSchema:
        def validate_from_str(v: str) -> ObjectId:
            if not ObjectId.is_valid(v):
                raise ValueError("Invalid ObjectId")
            return ObjectId(v)

        from_str_schema = core_schema.chain_schema(
            [
                core_schema.str_schema(),
                core_schema.no_info_plain_validator_function(validate_from_str),
            ]
        )

        return core_schema.json_or_python_schema(
            json_schema=core_schema.str_schema(),
            python_schema=core_schema.union_schema(
                [
                    core_schema.is_instance_schema(ObjectId),
                    from_str_schema,
                ]
            ),
            serialization=core_schema.plain_serializer_function_ser_schema(
                lambda instance: str(instance)
            ),
        )

# --- Modelos de Usuário ---
class UserBase(BaseModel):
    name: str = Field(..., example="João Silva")
    email: EmailStr = Field(..., example="joao.silva@email.com")

class UserCreate(UserBase):
    password: str = Field(..., min_length=6, example="senha123")

class UserInDB(UserBase):
    id: PyObjectId = Field(default_factory=PyObjectId, alias="_id")
    hashed_password: str
    reset_token: Optional[str] = None
    reset_token_expires: Optional[datetime] = None

    class Config:
        from_attributes = True
        populate_by_name = True

class UserPublic(UserBase):
    id: PyObjectId = Field(alias="_id")

    class Config:
        from_attributes = True
        populate_by_name = True

# --- Modelos de Token ---
class Token(BaseModel):
    access_token: str
    token_type: str

class TokenData(BaseModel):
    email: Optional[str] = None

# --- Modelos de Recuperação de Senha ---
class PasswordResetRequest(BaseModel):
    email: EmailStr

class PasswordReset(BaseModel):
    token: str
    new_password: str = Field(..., min_length=6)

# --- Modelos de Alerta ---
class AlertaNovoCreate(BaseModel):
    assetTicker: str = Field(..., example="PETR4")
    preco_alvo: float = Field(..., gt=0, example=55.80)
    tipo: str = Field(..., example="compra")

class AlertaNovoPublic(BaseModel):
    id: str
    userId: str
    assetTicker: str
    preco_alvo: float
    tipo: str
    ativo: bool
    
    class Config:
        from_attributes = True

class Dispositivo(BaseModel):
    token: str

# --- NOVOS MODELOS: Portfólio de Posições ---
class PositionBase(BaseModel):
    assetTicker: str = Field(..., example="PETR4")
    quantidade: int = Field(..., gt=0, example=100)
    preco_compra: float = Field(..., gt=0, example=30.50)

class PositionCreate(PositionBase):
    pass

class PositionPublic(PositionBase):
    id: str = Field(alias='_id')
    userId: str
    createdAt: datetime

    @field_validator('id', 'userId', mode='before')
    @classmethod
    def convert_objectid_to_str(cls, v):
        if isinstance(v, ObjectId):
            return str(v)
        return v
    
    class Config:
        from_attributes = True
        populate_by_name = True