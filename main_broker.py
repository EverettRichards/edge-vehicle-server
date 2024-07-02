# main_broker.py
import paho.mqtt.client as mqtt
import json
from collections import defaultdict as dd
import time


broker_IP = "localhost"
port_Num = 1883
last_verdict_time = 0.0

settings = {
    "broker_IP":broker_IP,
    "port_Num":port_Num,
    "verdict_min_refresh_time": 2.0, # Min number of seconds before a new verdict can be submitted
    "oldest_allowable_data": 2.5, # Max number of seconds before data is considered too old
    "show_verbose_output": True,
    "reputation_increment": 0.025, # Amount to increment or decrement client reputation by
    "min_reputation": 0.5, # Minimum reputation value
}

main_client = None

def clamp(value,min_value=0.0,max_value=1.0):
    return max(min_value, min(value, max_value))

def encodePayload(data):
    data["source"] = "main_broker"
    output = bytearray()
    output.extend(map(ord,json.dumps(data)))
    return output

def decodePayload(string_data):
    return json.loads(string_data)

def publish(CLIENT,topic,message):
    CLIENT.publish(topic,payload=encodePayload(message),qos=0,retain=False)

def on_connect(CLIENT, userdata, flags, rc):
    print(f"Connected with result code {rc}")
    # Subscribe to view incoming client messages
    CLIENT.subscribe("new_client")
    CLIENT.subscribe("end_client")
    # Subscribe to view incoming data from clients
    CLIENT.subscribe("data_V2B")

activeClients = []

class Decision:
    def __init__(self,decision_label,confidence,time_stamp):
        self.label = decision_label
        self.confidence = confidence
        self.time_stamp = time_stamp

    def getLabel(self):
        return self.label
    
    def getConfidence(self):
        return self.confidence
    
    def getTimeStamp(self):
        return self.time_stamp
    
    def setLabel(self,decision_label):
        self.label = decision_label

    def setConfidence(self,confidence):
        self.confidence = confidence

    def setTimeStamp(self,time_stamp):
        self.time_stamp = time_stamp
    
    def __str__(self):
        return self.label + ": " + str(self.confidence)

class Client:
    def __init__(self,client_name):
        self.name = client_name
        self.decision = None
        self.reputation = 0.5

    def makeDecision(self,decision):
        self.decision = decision

    def getDecision(self):
        return self.decision
    
    def setDecision(self,decision):
        self.decision = decision
    
    def getReputation(self):
        return self.reputation
    
    def noteOutcome(self,my_vote,verdict):
        if my_vote == verdict:
            self.reputation += settings["reputation_increment"]
        else:
            self.reputation -= settings["reputation_increment"]
        self.reputation = clamp(self.reputation,settings["min_reputation"],1)

    def getName(self):
        return self.name

    def __str__(self):
        return self.name + ": " + str(self.decision)

    def __repr__(self):
        return self.name + ": " + str(self.decision)

def initializeClient(client_name):
    try:
        for client in activeClients:
            if client.name == client_name:
                raise Exception("Client already exists")
        new_client = Client(client_name)
        activeClients.append(new_client)
        print("Added client: ",client_name)
        return new_client
    except:
        print("Failed to add client. Client already exists: ",client_name)

def removeClient(client_name):
    try:
        for client in activeClients:
            if client.name == client_name:
                activeClients.remove(client)
                print("Removed client: ",client_name)
                return
        raise Exception("Client not found")
    except:
        print("Failed to remove client. Client not found: ",client_name)

def getVerdict():
    global last_verdict_time
    NOW = time.time()
    if (NOW - last_verdict_time) < settings["verdict_min_refresh_time"]:
        return
    if settings["show_verbose_output"]:
        print("-"*40)
        print("Getting verdict for t =",NOW)
        print("-"*40)
    # Refresh the last verdict time
    last_verdict_time = NOW
    # Initialize a blank Default Dictionary
    counts = dd(int)

    # Count the number of each decision made
    for client in activeClients:
        decision = client.getDecision()
        # Throw out expired decisions
        if decision == None or decision.getTimeStamp() < NOW - settings["oldest_allowable_data"]:
            continue
        # Add the decision, using confidence level as the weight
        counts[decision.getLabel()] += decision.getConfidence() * client.getReputation()
        # Verbose output
        if settings["show_verbose_output"]:
            print(f"---@{client.getName()} (rep={client.getReputation():.3f}): {decision.getLabel()} (conf={(decision.getConfidence()*100):.1f}%)")
    
    # Determine the most confident decision
    verdict = f"{max(counts,key=counts.get)}"
    
    # Publish the verdict
    publish(main_client,"verdict",{"message":verdict})
    print("Submitted verdict: ",verdict)

    # Update client reputations using Client object methods
    wrong_decision_count = 0
    for client in activeClients:
        if client.getDecision() == None:
            continue
        client.noteOutcome(client.getDecision().getLabel(),verdict)
        if client.getDecision().getLabel() != verdict:
            wrong_decision_count += 1
    print("# of clients who had their minds changed: ",wrong_decision_count)

def didEveryoneDecide():
    for client in activeClients:
        if client.getDecision() == None:
            return False
    return True

def getClientByName(client_name):
    for client in activeClients:
        if client.getName() == client_name:
            return client
    return None

def interpretData(payload):
    client = getClientByName(payload["source"])
    if client == None:
        print("Attempting to create new client,",payload["source"])
        client = initializeClient(payload["source"])
        if client == None:
            print("Failed to create new client")
            return
    decision = Decision(payload["label"],payload["confidence"],time.time())
    client.setDecision(decision)
    if time.time() - last_verdict_time > settings["verdict_min_refresh_time"]:
        getVerdict()

# The callback function, it will be triggered when receiving messages
def on_message(CLIENT, userdata, msg):
    # Turn from byte array to string text
    payload = msg.payload.decode("utf-8")
    # Turn from string text to data structure
    payload = decodePayload(payload)
    # Decide what to do, based on the message's topic
    if msg.topic == "new_client":
        # Add a new client!
        initializeClient(payload["source"])
    elif msg.topic == "end_client":
        # Remove an existing client. Sad!
        removeClient(payload["source"])
    elif msg.topic == "data_V2B":
        # Interpret the data
        interpretData(payload)

CLIENT = mqtt.Client()
CLIENT.on_connect = on_connect
CLIENT.on_message = on_message
main_client = CLIENT

# Set the will message, when the Raspberry Pi is powered off, or the network is interrupted abnormally, it will send the will message to other clients
CLIENT.will_set('msg_B2V', encodePayload({"message":"I'm offline"}), qos=0, retain=False)

# Create connection, the three parameters are broker address, broker port number, and keep-alive time respectively
CLIENT.connect(broker_IP, port_Num, keepalive=60)

# Set the network loop blocking, it will not actively end the program before calling disconnect() or the program crash
CLIENT.loop_forever()