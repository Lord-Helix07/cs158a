# =============================================================================
# CS 158A  -  Programming Assignment 2
# Leader Election on an Asynchronous Ring
#
# NAME: Shivaji Ganesh
# =============================================================================

import argparse
import json
import socket
import threading
import time
import uuid


class Message:
    """Election message. Field names uuid and flag must not be changed."""

    def __init__(self, uuid, flag):
        self.uuid = uuid
        self.flag = flag  # 0 = electing, 1 = leader already chosen

    def to_json(self):
        return json.dumps(
            {
                "uuid": str(self.uuid),
                "flag": self.flag
            }
        )

    @classmethod
    def from_json(cls, text):
        return cls.from_data(json.loads(text))

    @classmethod
    def from_data(cls, data):
        return cls(
            uuid.UUID(str(data["uuid"])),
            int(data["flag"]),
        )


def load_config(path):
    """config.txt: line 1 = my listen address, line 2 = neighbor to connect to."""
    with open(path, "r", encoding="utf-8") as f:
        lines = [line.strip() for line in f if line.strip()]
    listen_ip, listen_port = lines[0].split(",", 1)
    connect_ip, connect_port = lines[1].split(",", 1)
    return (
        listen_ip.strip(),
        int(listen_port.strip()),
        connect_ip.strip(),
        int(connect_port.strip()),
    )


class ElectionNode:
    def __init__(self, config, log_path, wait):
        self.listen_ip, self.listen_port, self.connect_ip, self.connect_port = config
        self.log_path = log_path
        self.wait = wait
        self.my_id = uuid.uuid4()
        self.state = 0  # 0 = electing, 1 = knows the leader
        self.leader_id = None
        self.recv_sock = None
        self.send_sock = None
        self._accepted = threading.Event()
        self._accept_error = None
        with open(self.log_path, "w", encoding="utf-8") as f:
            f.write("")

    def log(self, line):
        if not line.endswith("\n"):
            line += "\n"
        with open(self.log_path, "a", encoding="utf-8") as f:
            f.write(line)
        print(line, end="", flush=True)

    def _cmp(self, other):
        if other > self.my_id:
            return "greater"
        if other < self.my_id:
            return "less"
        return "same"

    def _server_loop(self):
        # accept() in its own thread so it cannot block connect()
        try:
            server_sock = socket.socket(
                socket.AF_INET,
                socket.SOCK_STREAM
            )
            server_sock.setsockopt(
                socket.SOL_SOCKET,
                socket.SO_REUSEADDR,
                1
            )
            try:
                server_sock.bind((self.listen_ip, self.listen_port))
            except OSError:
                server_sock.bind(("0.0.0.0", self.listen_port))
            server_sock.listen(1)
            self.log(
                f"Listening on {self.listen_ip}:{self.listen_port}"
            )
            self.recv_sock, address = server_sock.accept()
            self.log(
                f"Accepted connection from {address[0]}:{address[1]}"
            )
        except BaseException as exc:
            self._accept_error = exc
        finally:
            self._accepted.set()

    def _connect_client(self):
        last_error = None
        for _ in range(120):
            sock = socket.socket(
                socket.AF_INET,
                socket.SOCK_STREAM
            )
            try:
                sock.connect(
                    (self.connect_ip, self.connect_port)
                )
                self.log(
                    f"Connected to "
                    f"{self.connect_ip}:{self.connect_port}"
                )
                return sock
            except OSError as exc:
                last_error = exc
                sock.close()
                time.sleep(0.25)
        raise ConnectionError(
            f"Could not connect to "
            f"{self.connect_ip}:{self.connect_port}: "
            f"{last_error}"
        )

    def _send(self, message):
        # JSON already ends with '}', which marks the end of a message
        encoded = message.to_json().encode("utf-8")
        self.send_sock.sendall(encoded)
        self.log(
            f"Sent: uuid={message.uuid}, "
            f"flag={message.flag}"
        )

    def _handle(self, message):
        comparison = self._cmp(message.uuid)
        received = (
            f"Received: uuid={message.uuid}, "
            f"flag={message.flag}, "
            f"{comparison}, "
            f"{self.state}"
        )
        if self.state == 1:
            received += f", leader={self.leader_id}"
        self.log(received)

        if message.flag == 1:
            if self.state == 1:
                self.log(
                    f"Ignored: uuid={message.uuid}, "
                    f"flag={message.flag}"
                )
                return
            self.leader_id = message.uuid
            self.state = 1
            self.log(
                f"Leader is decided to {self.leader_id}."
            )
            print(
                f"leader is {self.leader_id}",
                flush=True
            )
            self._send(message)
            return

        if self.state == 1:
            self.log(
                f"Ignored: uuid={message.uuid}, "
                f"flag={message.flag}"
            )
            return

        if message.uuid > self.my_id:
            self._send(message)
        elif message.uuid < self.my_id:
            self.log(
                f"Ignored: uuid={message.uuid}, "
                f"flag={message.flag}"
            )
        else:
            # received our own UUID — we are the leader
            self.leader_id = self.my_id
            self.state = 1
            self.log(
                f"Leader is decided to {self.leader_id}."
            )
            print(
                f"leader is {self.leader_id}",
                flush=True
            )
            self._send(
                Message(
                    self.my_id,
                    flag=1
                )
            )

    def run(self):
        self.log(
            f"Process id: {self.my_id}"
        )

        server_thread = threading.Thread(
            target=self._server_loop,
            daemon=True
        )
        server_thread.start()
        time.sleep(1.0)

        if self.wait:
            input(
                "press Enter when everyone is ready."
            )

        self.send_sock = self._connect_client()
        self._accepted.wait()
        if self._accept_error:
            raise self._accept_error

        # send our UUID once, with no comparison
        self._send(
            Message(
                self.my_id,
                flag=0
            )
        )

        buffer = ""
        while True:
            chunk = self.recv_sock.recv(4096)
            if not chunk:
                return
            buffer += chunk.decode("utf-8")
            # '}' marks the end of a message
            while "}" in buffer:
                raw, buffer = buffer.split("}", 1)
                raw = (raw + "}").strip()
                if raw:
                    message = Message.from_json(
                        raw
                    )
                    self._handle(message)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config",
        default="config.txt"
    )
    parser.add_argument(
        "--log",
        default="log.txt"
    )
    parser.add_argument(
        "--wait",
        action="store_true"
    )
    args = parser.parse_args()

    config = load_config(args.config)
    node = ElectionNode(
        config,
        args.log,
        args.wait
    )
    node.run()


if __name__ == "__main__":
    main()
