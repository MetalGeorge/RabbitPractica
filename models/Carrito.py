from pydantic import BaseModel
from typing import List
from models.Producto import Producto
class Carrito(BaseModel):

    numero: int
    productos: List[Producto]