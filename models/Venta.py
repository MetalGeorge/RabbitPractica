
from typing import List
from models.Producto import Producto
from pydantic import BaseModel

class Venta(BaseModel):

    numero: int
    productos: List[Producto]
    subtotal: float
    iva: float
    total: float

