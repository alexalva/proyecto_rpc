import rpyc
import random
import time

def leer_configuracion(archivo_config='config.txt'):
    """Lee la dirección IP y el puerto desde un archivo de configuración."""
    with open(archivo_config, 'r') as file:
        ip, port = file.read().strip().split('\n')
    return ip, int(port)

def productor(productor_id):
    # ip, port = leer_configuracion()
    conn = rpyc.connect("localhost", 20002, config={"sync_request_timeout": 60})  # Asegúrate de cambiar a la dirección IP y puerto correctos
    keys = ['clave1', 'clave2', 'clave3']
    operations = ['set', 'add', 'mult']

    while True:
        key = random.choice(keys)
        value = random.randint(1, 10)
        operation = random.choice(operations)  # Incluye 'get' en la lista de operaciones posibles

        if operation == 'get':
            # Para operaciones get, llama a un método diferente y maneja el valor devuelto
            print(f"Productor {productor_id}: Solicitando valor para clave: {key}")
            result = conn.root.exposed_get(key)  # Usa el método 'exposed_get' para operaciones get
            print(f"Resultado de get para {key}: {result}")
        else:
            # Para otras operaciones, usa el método 'exposed_update'
            print(f"Productor {productor_id}: Enviando operación: {operation} {key}={value}")
            conn.root.exposed_update(key, value, operation)

        time.sleep(random.randint(15, 20))  # Espera entre 1 y 3 segundos antes de enviar la siguiente operación

if __name__ == "__main__":
    productor_id = input("Ingrese el ID del productor: ")
    productor(productor_id)
