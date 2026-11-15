"""
COMPONENTE: Generador de Eventos (data_gen/e_commerce_data_gen.py)
OBJETIVO: Enviar compras simuladas en formato JSON a Kafka continuamente.
TIEMPO ESTIMADO DE IMPLEMENTACIÓN: 15 minutos
"""

# ==============================================================================
# PSEUDOCÓDIGO / GUÍA DE IMPLEMENTACIÓN (15-20 líneas de código real)
# ==============================================================================
#
# 1. IMPORTAR:
import json, time, random
from datetime import datetime
from kafka import KafkaProducer
#
# 2. INICIALIZAR PRODUCTOR:
producer = KafkaProducer(
       bootstrap_servers=['localhost:29092'],
       value_serializer=lambda v: json.dumps(v).encode('utf-8')
   )
#
# 3. BUCLE INFINITO DE ENVÍO:
while True:
       evento = {
           "user_id": random.randint(1, 100),
           "product": random.choice(["Laptop", "Móvil", "Auriculares", "Teclado"]),
           "price": round(random.uniform(20.0, 500.0), 2),
           "timestamp": datetime.now().isoformat()
       }
       producer.send('compras_topic', value=evento)
       print(f"Evento enviado: {evento}")
       time.sleep(1)  # 1 evento por segundo
