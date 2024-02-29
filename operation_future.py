import threading

class OperationFuture:
    def __init__(self):
        self.result = None
        self.exception = None
        self.event = threading.Event()

    def set_result(self, result):
        self.result = result
        self.event.set()

    def set_exception(self, exception):
        self.exception = exception
        self.event.set()

    def get_result(self):
        self.event.wait()  # Espera hasta que el resultado esté disponible
        if self.exception:
            raise self.exception
        return self.result

