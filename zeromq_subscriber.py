import zmq
from zmq import ZMQError
import time

class Subscriber():    
    def __init__(self, name, address, processing_time, id):
        self.name = name
        self.id = id
        self.address = address
        self.processing_time = processing_time

        self.read_messages = 0
        
        #self.socket = zmq.Context().socket(zmq.SUB)

    def read_message(self):
        #msg = None
        
        #event = self.socket.poll(timeout=0)

        #msg = self.socket.recv_pyobj()
        #msg = self.socket.recv_string()
        #print(f"\n|{event}|\n")

        #if event == 0:
        #    pass
        msg = self.socket.recv()

        if (msg):
            self.read_messages += 1
            time.sleep(self.processing_time / 1000)
            message = f"\n|{self.name}| recieved from {self.address}: {msg}, Read:{self.read_messages}"
            print (message)
            
            return message
        
        return False

    def set_socket(self, socket):
        self.socket = socket
        #self.socket.connect(self.address)
        #self.socket.bind(self.address)

        try:
            self.socket.bind(self.address)

        except ZMQError:
            self.socket.connect(self.address)
            print("CONNECT")

        self.socket.setsockopt_string(zmq.SUBSCRIBE, '')
        
        
        #self.socket.setsockopt_string(zmq.SUBSCRIBE, "tipa_string")

##
"""
sub = Subscriber("pyatachock", "tcp://127.0.0.1:2007", 1.0, 1)
new_socket = zmq.Context().socket(zmq.SUB)
sub.set_socket(new_socket)

sub2 = Subscriber("clone", "tcp://127.0.0.1:2007", 1.0, 1)
sub2.set_socket(new_socket)

log_file1 = open(f'logs/{"sub1"}.txt', 'w')
log_file2 = open(f'logs/{"sub2"}.txt', 'w')

while (True):
    sub2.read_message()
    sub.read_message()

    log_file1.write(f"\n{time.time()}\n")
    log_file1.flush()

    time.sleep(0.1)

    log_file2.write(f"\n{time.time()}\n")
    log_file2.flush()

    time.sleep(0.1)
"""