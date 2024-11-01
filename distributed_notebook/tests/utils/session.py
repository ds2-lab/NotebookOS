from jupyter_client.session import Session

class TestSession(Session):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        print("Created new Test Session")

    def send(self, *args, **kwargs):
        print(f"TestSession::send called with args {args} and kwargs {kwargs}.")