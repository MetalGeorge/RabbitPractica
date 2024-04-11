
from pydantic import BaseModel
class Producto(BaseModel):

    name: str
    stock: int
    price: float