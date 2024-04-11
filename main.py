from fastapi import FastAPI, BackgroundTasks
from typing import List
from celery import Celery
from models.Carrito import Carrito
from models.Producto import Producto
import json

app = FastAPI()

celery = Celery('tasks', broker='amqp://admin:admin@localhost:5672//',backend='rpc://')

@app.post("/add")
async def send_message_api(carrito: Carrito):
    # products_dict = [product.dict() for product in products]
    result = celery.send_task("tasks.send_carrito", args=[carrito.dict()], queue="carrito")
    return {"status": "Message sent to Celery for processing", "task_id": result.id}



@app.post("/productos")
async def send_message_api(producto: Producto):
    # products_dict = [product.dict() for product in products]
    result = celery.send_task("tasks.send_producto", args=[producto.dict()], queue="productos")
    return {"status": "Message sent to Celery for processing prodcuts", "task_id": result.id}


@app.post("/venta")
async def send_generar_venta(carrito: Carrito):
    # products_dict = [product.dict() for product in products]
    result = celery.send_task("tasks.send_tienda", args=[carrito.dict()], queue="tienda")
    return {"status": "Message sent to Celery for processing", "task_id": result.id}
