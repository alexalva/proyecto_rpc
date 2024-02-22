import rpyc
import random
import time

def productor():
    conn = rpyc.connect("localhost", 18812)
    keys = ['clave1', 'clave2', 'clave3']
    operations = ['set', 'add', 'mult']
    
    while True:
        key = random.choice(keys)
        value = random.randint(1, 10)
        operation = random.choice(operations)
        
        print(f"Enviando operación: {operation} {key}={value}")
        conn.root.exposed_update(key, value, operation)
        
        time.sleep(random.randint(1, 3))  # Espera entre 1 y 3 segundos antes de enviar la siguiente operación

if __name__ == "__main__":
    productor()
