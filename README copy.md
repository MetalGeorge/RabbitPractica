# Steps for the API REST 1.0

## 1. Execute api with uvicorn
uvicorn main:app --reload 

## 2. To execute new queues 
celery -A <<file_worker>> worker --loglevel=info -Q <<name_queue>>

