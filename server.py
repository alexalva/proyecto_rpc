import rpyc
from StateMachine import StateMachine

class MyService(rpyc.Service):
    def __init__(self):
        self.sm = StateMachine()

    def exposed_read(self, key):
        return self.sm.get(key)

    def exposed_update(self, key, value, operation):
        if operation == 'set':
            return self.sm.set(key, value)
        elif operation == 'add':
            return self.sm.add(key, value)
        elif operation == 'mult':
            return self.sm.mult(key, value)
        else:
            return False

if __name__ == "__main__":
    from rpyc.utils.server import ThreadedServer
    t = ThreadedServer(MyService, port=18812)
    t.start()
