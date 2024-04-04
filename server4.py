# Servidor
import rpyc
from rpyc.utils.server import ThreadedServer
from StateMachine4 import StateMachine
from operation_future import OperationFuture
import threading
import time
import socket
import json
import argparse

def leer_configuracion(archivo_config):
    with open(archivo_config, 'r') as file:
        lines = [line.strip() for line in file if line.strip() and not line.startswith('#')]
    # Asume que la primera línea es para el servidor RPyC y la segunda línea es para el servidor de socket de réplica
    rpyc_address = tuple(lines[0].split(','))  
    rpyc_address = (rpyc_address[0], int(rpyc_address[1]))
    socket_address = tuple(lines[1].split(','))  
    socket_address = (socket_address[0], int(socket_address[1]))
    # El resto de las líneas son direcciones de réplicas
    replica_addresses = [tuple(line.split(',')) for line in lines[2:]]  
    replica_addresses = [(addr[0], int(addr[1])) for addr in replica_addresses]
    return rpyc_address, socket_address, replica_addresses



class MyService(rpyc.Service):
    def __init__(self, replica_addresses, my_address):
        self.sm = StateMachine()
        self.replica_addresses = replica_addresses  # Lista de direcciones IP de las réplicas
        self.my_address = my_address  # Dirección IP y puerto de este servidor
        # Iniciar un hilo para procesar elementos del buffer continuamente
        threading.Thread(target=self.process_buffer, daemon=True).start()
        threading.Thread(target=self.init_socket_server, daemon=True).start()

        # Inicia un nuevo hilo para imprimir los datos continuamente
        threading.Thread(target=self.print_data_continuously, daemon=True).start()

    def init_socket_server(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind(self.my_address)
        server_socket.listen()
        print(f"Escuchando en {self.my_address} para réplicas...")
        while True:
            client_socket, addr = server_socket.accept()
            print(f"Conexión entrante de {addr}")
            threading.Thread(target=self.handle_client_socket, args=(client_socket,), daemon=True).start()
    
    def handle_client_socket(self, client_socket):
        with client_socket:
            while True:
                data = client_socket.recv(1024)  # Tamaño del buffer 1024 bytes
                if not data:
                    break  # Conexión cerrada
                message = json.loads(data.decode('utf-8'))
                print(message)
                
                if message.get('type') == 'request_access':
                    # Manejar la solicitud de acceso aquí
                    self.handle_access_request(message, client_socket)  # Decodifica y carga JSON directamente

                elif message.get('type') == 'operation_consumed':
                    # Asegúrate de que el mensaje es para la próxima operación esperada
                    next_item = self.sm.peek()  # Utiliza el método peek que discutimos antes
                    print(f" Operacion consumida en otro servidor intentando consumir")
                    # if next_item and next_item[0] == message['timestamp']:
                    # Consume la operación del buffer
                    consumed_operation = self.sm.consume()
                    if consumed_operation:
                        timestamp, operation, key, value, future = consumed_operation

                        # Procesa la operación de manera similar a cómo lo hace process_buffer
                        try:
                            if operation == 'set':
                                result = self.sm.set(key, value)
                            elif operation == 'add':
                                result = self.sm.add(key, value)
                            elif operation == 'mult':
                                result = self.sm.mult(key, value)
                            elif operation == 'get':
                                result = self.sm.get(key)
                            else:
                                raise ValueError("Operación desconocida")

                            # if future is not None:
                            #     future.set_result(result)

                            print(f"Operación {operation} consumida y procesada con éxito.")

                        except Exception as e:
                            # if future is not None:
                            #     future.set_exception(e)
                            print(f"Error al procesar operación {operation}: {e}")

                    # Procesa la operación como se requiera
                    print(f"Operación consumida aqui mismo como servidor réplica: {consumed_operation}")
                    # else:
                    #     print("Inconsistencia detectada o mensaje de consumo adelantado recibido.")

                elif 'operation' in message:
                # Actualiza el reloj de Lamport con el sello de tiempo recibido
                    received_timestamp = message.get("timestamp", 0)  # Asume un valor por defecto si no se proporciona
                    self.sm.update_clock(received_timestamp)


                    operation = message['operation']['action']
                    key = message['operation']['key']
                    value = message['operation']['value']

                    # Incrementa el reloj de Lamport antes de encolar la operación
                    timestamp = self.sm.increment_clock()

                    self.sm.produce(operation, key, value, timestamp)  # Encola la operación
                    print(f"Operación encolada. Tamaño del buffer ahora: {self.sm.buffer.qsize()}")
                else:
                    print("Mensaje no reconocido o mal formado.")


    def handle_access_request(self, message, client_socket):
        # Extraer el sello de tiempo de Lamport y la dirección del servidor solicitante del mensaje
        request_timestamp = message['timestamp']
        requesting_server = message['server_address']

        print(f"Recibida solicitud de acceso de {requesting_server} con timestamp {request_timestamp}.")


         # Utiliza peek para obtener el sello de tiempo del próximo item sin removerlo
        next_item = self.sm.peek()
        next_timestamp = next_item[0] if next_item else float(0)

        # Actualizar el reloj de Lamport basado en el sello de tiempo recibido
        self.sm.update_clock(request_timestamp)
        print(f"Reloj de Lamport actualizado a {self.sm.lamport_clock} tras recibir solicitud.")

        # Decidir si conceder o no el acceso
        # Este ejemplo concede el acceso de manera predeterminada, pero deberías implementar tu lógica
        # específica de control de acceso aquí. Esto podría incluir comprobaciones como:
        # - Si el servidor ya está realizando una operación crítica
        # - Si hay operaciones pendientes con sellos de tiempo menores, etc.
        access_granted = request_timestamp <= next_timestamp # Esta línea es solo un marcador de posición

        print(f"Solicitud Timestamp: {request_timestamp}")
        print(f"El timestamp de mi buffer: {next_timestamp}")

        if access_granted:
            print("Acceso concedido basado en la comparación de timestamps.")
        else:
            print(f"Acceso denegado hacia {requesting_server}. La operación entrante con tiempo {request_timestamp} no es la próxima esperada basada en el timestamp.")

        # Construir el mensaje de respuesta
        response_message = json.dumps({
            'type': 'access_response',
            'approval': access_granted,
            'timestamp': self.sm.increment_clock(),  # Incrementar el reloj de Lamport para este evento
            'responding_server': self.my_address
        })

        # Enviar la respuesta al servidor solicitante
        try:
            client_socket.sendall(response_message.encode('utf-8'))
        except Exception as e:
            print(f"Error al enviar la respuesta de acceso a {requesting_server}: {e}")


    def replicate_to_peers(self, operation, key, value):
        # Obtén el sello de tiempo de Lamport actual
        timestamp = self.sm.increment_clock()

        # Construye el mensaje incluyendo el sello de tiempo de Lamport
        message = json.dumps({
            "operation": {
                "action": operation,
                "key": key,
                "value": value
            },
            "timestamp": timestamp  # Incluye el sello de tiempo de Lamport aquí
        })
        for address in self.replica_addresses:
            try:
                with socket.create_connection(address, timeout=10) as sock:
                    sock.sendall(message.encode('utf-8'))
            except Exception as e:
                print(f"Error replicando a {address}: {e}")

    def exposed_read(self, key):
        # Esta operación puede permanecer igual, ya que get no necesita interactuar con el buffer
        return self.sm.get(key)

    def exposed_update(self, key, value, operation):
        result = self.sm.produce(operation, key, value)
        if result:
            self.replicate_to_peers(operation, key, value)
        return result

    def process_buffer(self):
        # Hilo que consume operaciones del buffer y las aplica
        while True:
            print("Intentando Consumir...")
            if self.request_access_to_consume():
                op = self.sm.consume()
                if op is not None:
                    # Asegúrate de que op tenga la longitud esperada antes de desempaquetar
                    if len(op) == 5:
                        timestamp, operation, key, value, future = op
                        print(f"Consumiendo operación: {operation} {key}={value} con timestamp {timestamp}")
                        # Simular un tiempo de procesamiento
                        time.sleep(10)  # Espera de 2 segundos para ilustrar el consumo lento

                        try:
                            # Realiza la operación y establece el resultado en el futuro
                            if operation == 'set':
                                result = self.sm.set(key, value)
                            elif operation == 'add':
                                result = self.sm.add(key, value)
                            elif operation == 'mult':
                                result = self.sm.mult(key, value)
                            elif operation == 'get':
                                result = self.sm.get(key)
                            else:
                                raise ValueError("Operación desconocida")

                            # Si la operación tiene un futuro asociado, establece el resultado
                            if future is not None:
                                future.set_result(result)

                            self.confirm_operation_consumption(timestamp)

                        except Exception as e:
                            # Si ocurre un error durante la operación, establece la excepción en el futuro
                            if future is not None:
                                future.set_exception(e)

                    else:
                        print("Operación consumida no tiene el formato esperado.")
            else:
                print("Esperando aprobación para consumir del buffer...")  # Manejar el caso en el que consume devuelva None (por ejemplo, esperando un poco antes de reintentar)
            time.sleep(30)

    def request_access_to_consume(self):
        # Incrementar el reloj de Lamport para esta acción
        my_timestamp = self.sm.increment_clock()
        next_item = self.sm.peek()
        next_timestamp = next_item[0] if next_item else float(0)

        # Construir el mensaje de solicitud
        request_message = json.dumps({
            'type': 'request_access',
            'timestamp': next_timestamp,
            'server_address': self.my_address
        })

        responses = []

        # Enviar solicitud de acceso a todos los servidores réplica
        for address in self.replica_addresses:
            try:
                # Abrir conexión con el servidor réplica
                with socket.create_connection(address, timeout=5) as sock:
                    # Enviar solicitud
                    sock.sendall(request_message.encode('utf-8'))
                    
                    # Recibir respuesta
                    response = sock.recv(1024).decode('utf-8')
                    response_data = json.loads(response)
                    
                    # Verificar si la respuesta es de aprobación
                    if response_data.get('type') == 'access_response' and response_data.get('approval'):
                        responses.append(True)
                    else:
                        responses.append(False)
            except Exception as e:
                print(f"Error al enviar solicitud de acceso a {address}: {e}")
                responses.append(False)

        # Verificar si todas las respuestas son afirmativas
        if all(responses):
            print("Acceso al buffer concedido por todos los servidores.")
            return True
        else:
            print("Acceso al buffer denegado por al menos un servidor.")
            return False
        
    def confirm_operation_consumption(self, operation_timestamp):
        confirmation_message = json.dumps({
            'type': 'operation_consumed',
            'timestamp': operation_timestamp,
            'server_address': self.my_address
        })
        # Envía este mensaje a todos los servidores réplica
        for address in self.replica_addresses:
            try:
                with socket.create_connection(address, timeout=10) as sock:
                    sock.sendall(confirmation_message.encode('utf-8'))
            except Exception as e:
                print(f"Error enviando confirmación de consumo a {address}: {e}")
    
    def exposed_get(self, key):
        """
        Accede directamente al estado para obtener el valor de una clave.
        Este método no encola la solicitud y devuelve el valor inmediatamente.
        """
        return self.sm.get(key)

    # def exposed_get(self, key):
    #     future = OperationFuture()
    #     self.sm.produce('get', key, None, future)
    #     return future.get_result()
    
    def print_data_continuously(self):
        while True:
            with self.sm.semaphore:  # Asegurar acceso exclusivo al diccionario 'data'
                print("Valores actuales en el diccionario 'data':")
                for key in ['clave1', 'clave2', 'clave3']:
                    value = self.sm.data.get(key, 'No definido')
                    print(f"  {key}: {value}")
            time.sleep(5)  # Espera 5 segundos antes de imprimir nuevamente




if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Servidor RPyC con configuración de réplicas.')
    parser.add_argument('config_file', type=str, help='Archivo de configuración del servidor.')
    args = parser.parse_args()

    # Leer configuración del servidor
    rpyc_address, socket_address, replica_addresses = leer_configuracion(args.config_file)
    
    # Crear instancia del servicio con la configuración leída
    service = MyService(replica_addresses, socket_address)
    
    # Iniciar el servidor RPyC con la instancia de servicio y el puerto correcto
    t = ThreadedServer(service, port=rpyc_address[1])
    t.start()

