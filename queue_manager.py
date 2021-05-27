import json
import zmq
#import datetime
import time
import random

from threading import Thread
from zeromq_publisher import Publisher
from zeromq_subscriber import Subscriber


data = None
all_members = []

max_simulation_time = 0
simulation_multiplier = None
before_simulation = time.time()

class Member():
    def __init__(self, name, queues, log_file, divider):
        self.name = name
        self.subscribers = []
        self.publishers = []
        self.thread = Thread(target=self.start_queues)

        self.total_time = 0
        self.total_messages = 0
        self.previous_message_count = 0
        self.messages_read = 0

        self.then = time.time()
        self.now = time.time()
        self.log_file = log_file
        self.divider = divider
        self.address = "tcp://127.0.0.1:"

        for queue in queues:
            id = 0
        
            if ("id" in queue):
                    id = queue["id"]

            address = self.address + str(queue["port"])

            if (queue["role"] == "publisher"):
                new_publisher = Publisher(self.name, address, queue["type"], float(queue["rate"] / self.divider), id)
                self.publishers.append(new_publisher)
                
            elif (queue["role"] == "subscriber"):
                
                new_subscriber = Subscriber(self.name, address, queue["processing_time_ms"], id)
                self.subscribers.append(new_subscriber)

    def send_log(self, message):
        self.log_file.write(f"{message}")
        self.log_file.flush()

    def send_message(self, pub):
        self.now = time.time()
        self.total_time = self.now - self.then
        send_result = pub.send_message(self.total_time)

        self.send_log(send_result)
        self.total_messages += 1
        
        #total_result = f"|{self.name} total messages: {self.total_messages}|"
        #total_result = f"|{self.total_messages}|"
        #total_result = f"|Total: {self.total_messages}, Pub: {pub.sended_messages}, Coeff: |{pub.rate / (pub.sended_messages/self.total_time) * 2}|"
        #total_result = f"|Total: {self.total_messages}, Pub: {pub.sended_messages}, Coeff: |{pub.sended_messages/self.total_time}|"
        
        # Слава индусскому коду
        rate_summ = 0

        for publisher in self.publishers:
            rate_summ += publisher.rate
        """
        
    
        coeff = 0
        if (self.messages_read > 0):
            coeff = self.total_messages / self.messages_read / rate_summ / simulation_multiplier / 2
        """

        coeff = pub.sended_messages / self.total_time / rate_summ / simulation_multiplier
        total_result = f"|Total: {self.total_messages}, Count: {pub.sended_messages}, Avg.Rate: |{coeff}|"

        self.send_log(total_result)

    def start_queues(self):
        print(f"|||{self.name} is created.|||\n")
        global max_simulation_time

        while(self.total_time < max_simulation_time):
            #print(f"||{simulation_time}||{max_simulation_time}||")
            
            self.now = time.time()
            self.total_time = self.now - self.then
            #self.total_time = simulation_time
            #n = 0
            
            for sub in self.subscribers:
                if (self.total_time >= max_simulation_time): break;
                read_result = sub.read_message()
                
                if (read_result == False): continue

                self.messages_read +=1
                self.send_log(read_result)
                #self.log_file.write(read_result)
                
                for pub in self.publishers:
                    # Постинг сообщения только если публикатор имеет сходный id вместе с подписчиком
                    if pub.type == "fixed" and pub.id == sub.id and pub.id != 0:
                        
                        new_rate = pub.rate
                        remainder = new_rate - new_rate // 1
                        
                        if (new_rate // 1 >= 1):
  
                            for i in range(int(new_rate // 1)):
                                ###
                                if (self.total_time >= max_simulation_time):
                                    return;
                                ###
                                self.send_message(pub)

                        if (random.uniform(0, 1) < remainder):                            
                            self.send_message(pub)

            for pub in self.publishers:
                #n += 1
                #self.send_log(f"\n{n}\n")
                if (self.total_time >= max_simulation_time): break;

                pub.dt = time.time() - pub.prev
                pub.prev = time.time()

                if pub.type == "random":
                    new_rate = pub.rate * pub.dt * simulation_multiplier
                    
                    if (new_rate // 1 >= 1):
                        remainder = new_rate - new_rate // 1

                        for i in range(int(new_rate // 1)):
                            ###
                            if (self.total_time >= max_simulation_time):
                                return;
                            ###
                            self.send_message(pub)

                        new_rate = remainder

                    if (random.uniform(0, 1) < new_rate):
                        self.send_message(pub)


        print(f"Thread {self.name} disabled.")

def get_config():
    global data
    with open("config.json", "r", encoding='utf-8') as read_file:
        data = json.load(read_file)


def generate_members():
    global max_simulation_time
    global simulation_multiplier
    print("generating members...")

    for member in data["members"]:
        
        log_file = None
    
        #if (len(member["queues"]) > 0):
        

        # Взятие количество копий звена
        member_copies = 0
        try:
            member_copies = member["count"]

        except KeyError:
            member_copies = 1
        
        sub_socket = zmq.Context().socket(zmq.SUB)

        print(member_copies)
        for i in range(member_copies):

            if (member_copies == 1):
                log_file = open(f'logs/{member["name"]}.txt', 'w')
            
            else:
                log_file = open(f'logs/{member["name"]}{i + 1}.txt', 'w')

            new_member = Member(member["name"], member["queues"], log_file, member_copies)
            all_members.append(new_member)

            # Назначение сокетов для публикаторов
            for pub in new_member.publishers:
                socket = zmq.Context().socket(zmq.PUB)
                pub.set_socket(socket)

            # Назначение сокетов для подписчиков
            for sub in new_member.subscribers:                
                sub.set_socket(sub_socket)

    max_simulation_time = data["parameters"]["simulation_time"]      
    simulation_multiplier = data["parameters"]["simulation_multiplier"]


get_config()
generate_members()

for memb in all_members:
    memb.thread.start()
    #memb.thread.join()
    

