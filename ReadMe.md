# Proyecto RPC
 Proyecto RPC de Cómputo Distribuido

## Configuración del Servidor
Ejecuta el Servidor en una Máquina:

Elige una máquina en tu LAN para actuar como servidor.
Asegúrate de que el firewall de la máquina del servidor permita conexiones entrantes en el puerto que estás utilizando (por ejemplo, el puerto 18812).

### Obtén la Dirección IP del Servidor:

Necesitas la dirección IP de la máquina del servidor dentro de tu LAN. Puedes obtenerla generalmente ejecutando ipconfig (en Windows) o ifconfig (en sistemas basados en Unix) en una terminal.
Asegúrate de que esta IP sea accesible desde otras máquinas en tu LAN.

## Configuración del Cliente
Ejecuta el Cliente en una Máquina Diferente:

En la máquina que actuará como cliente, asegúrate de que el script del cliente (client.py) esté disponible.
Modifica el Código del Cliente para Conectarse al Servidor:

En client.py, en lugar de conectarte a "localhost", debes conectarte a la dirección IP del servidor. Por ejemplo:
python
Copy code
conn = rpyc.connect("192.168.1.10", 18812)  # Reemplaza con la IP real del servidor
Asegúrate de que el puerto (18812 en tu caso) sea el mismo que el servidor está escuchando.