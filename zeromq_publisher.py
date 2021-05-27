#import datetime
import zmq
import time
from zmq.error import ZMQError

class Publisher():
    def __init__(self, name, address, type, rate, id):
        self.name = name
        self.id = id
        self.type = type
        self.rate = rate
        self.address = address

        self.dt = 0
        self.prev = time.time()
        self.sended_messages = 0
        
    def send_message(self, time):
        self.socket.send_string(f"{time}, {self.name}")
        #self.socket.send_string('Child: %s %s' % (time, self.name))
        result = f"\n|{self.name}| sended to {self.address}: {time}. Sended: {self.sended_messages}"
        print(result)
        self.sended_messages += 1

        return result
    
    def set_socket(self, socket):
        self.socket = socket
        self.socket.connect(self.address)

        """
        try:
            self.socket.bind(self.address)

        except ZMQError:
            self.socket.connect(self.address)
            print("CONNECT")
        """ 

##
"""
pub1 = Publisher("name1", "tcp://127.0.0.1:2007", "random", 100.0, 1)
#pub2 = Publisher("name2", "tcp://127.0.0.1:2002", "random", 100.0, 2)

pub1.set_socket (zmq.Context().socket(zmq.PUB))
#pub2.set_socket (zmq.Context().socket(zmq.PUB))

while (True):
    pub1.send_message(time.time())
    #pub2.send_message()
"""
