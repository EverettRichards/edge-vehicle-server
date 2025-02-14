# main_broker.py
import paho.mqtt.client as mqtt
import json
from collections import defaultdict as dd
import time
import numpy as np
from colors import *
from server_config import config as settings

broker_IP = "localhost"
port_Num = 1883
last_verdict_time = 0.0

client_config_file = open("client_config.json","r")
client_config_str = client_config_file.read()
client_config_data = json.loads(client_config_str)
client_config_file.close()

object_locations = client_config_data["object_locations"]
vehicle_locations = client_config_data["vehicle_locations"]

NoneObject = ["None",0.1,0.0]

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
    
    def noteOutcome(self,verdicts):
        output = 0
        try:
            # Don't do anything if you made NO decisions
            if self.getDecision() == None:
                return
            # Get list of this client's decisions
            decisions = self.getDecision()["object_list"]
            # Compare decisions to actual verdicts. -1 = disagree, 0 = no true verdict, 1 = agree
            comparisons = [(float(decisions[obj][0] == verdicts[obj] if verdicts[obj] != "None" else 0.5)-0.5)*2 for obj in object_locations.keys()]
            # Increment (or decrement) reputation based on comparisons
            print("SUM COMP: ",sum(comparisons))
            print(comparisons)
            self.reputation = clamp(self.reputation + sum(comparisons) * settings["reputation_increment"], settings["min_reputation"], 1)
            output = len([c for c in comparisons if c < -0.5])
        except Exception as e:
            print(e)
        finally:
            return output

    def getName(self):
        return self.name

    def __str__(self):
        return self.name + ": " + str(self.decision)

    def __repr__(self):
        return self.name + ": " + str(self.decision)

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
    prCyan(f"Connected with result code {rc}")
    # Subscribe to view incoming client messages
    CLIENT.subscribe("new_client")
    CLIENT.subscribe("end_client")
    # Subscribe to view incoming data from clients
    CLIENT.subscribe("data_V2B")
    CLIENT.subscribe("request_config")

activeClients = []

def issueConfig():
    CLIENT.publish("config",payload=client_config_str,qos=0,retain=False)

def initializeClient(client_name):
    try:
        for client in activeClients:
            if client.name == client_name:
                raise Exception("Client already exists")
        new_client = Client(client_name)
        activeClients.append(new_client)
        issueConfig()
        prCyan("Added client: "+client_name)
        return new_client
    except:
        prRed("Failed to add client. Client already exists: "+client_name)

def removeClient(client_name):
    try:
        for client in activeClients:
            if client.name == client_name:
                activeClients.remove(client)
                prCyan("Removed client: "+client_name)
                return
        raise Exception("Client not found")
    except:
        prRed("Failed to remove client. Client not found: "+client_name)

def getVerdict():
    global last_verdict_time
    NOW = time.time()
    if (NOW - last_verdict_time) < settings["verdict_min_refresh_time"]:
        print(f"Returning. Now: {NOW}, Last: {last_verdict_time}")
        return
    
    # Refresh the last verdict time
    last_verdict_time = NOW

    # Initialize a list of blank Default Dictionaries to count occurrences of each decision
    global dd
    object_counts = {}
    for obj in object_locations.keys():
        object_counts[obj] = dd(int)

    # Clear the output log
    print("\033[H\033[J", end="")

    # Display separator for verdict presentation
    if settings["show_verbose_output"]:
        print("-"*40)
        print("Getting verdict for t ="+str(NOW))
        print("-"*40)

    # Count the number of each decision made
    for client in activeClients:
        decision = client.getDecision()
        # Throw out expired decisions
        if decision == None or decision["timestamp"] < NOW - settings["oldest_allowable_data"]:
            continue
        # Get the dictionary of detected objects
        detected_objects = decision["object_list"]
        # Add the decision, using confidence level as the weight
        #counts[decision.getLabel()] += decision.getConfidence() * client.getReputation()
        ##########################################
        # NEW LOGIC
        for obj in object_locations.keys():
            this_dd = object_counts[obj]
            chosen_obj = detected_objects[obj] or NoneObject
            this_dd[chosen_obj[0]] += chosen_obj[1] * client.getReputation() * (1/np.log(chosen_obj[2])) # Confidence * Reputation * (1/log(distance))
        ##########################################
        # Verbose output
        if settings["show_verbose_output"]:
            output_str = f"@{client.getName()} (rep={client.getReputation():.3f}):"
            for name,obj in detected_objects.items():
                if not obj: output_str += f" {name}=None ..."
                else: output_str += f" {name}={obj[0]} ({obj[1]*100:.1f}%) ..."
            prYellow(output_str)
    
    # Determine the most confident decisions for each object
    verdicts = {}
    for obj,this_dd in object_counts.items():
        verdicts[obj] = max(this_dd,key=this_dd.get)
    #verdict = f"{max(counts,key=counts.get)}"
    
    # Publish the verdict
    publish(main_client,"verdict",{"message":verdicts})

    print() # Get that nice, sweet newline!
    if settings["show_verbose_output"]:
        for obj in verdicts.keys():
            prGreen(f"$Object '{obj}' is: '{verdicts[obj]}'")
    else:
        prGreen("Submitted verdict: "+verdicts)

    if len(activeClients) > 1:
        # Update client reputations using Client object methods
        wrong_decision_count = 0
        for client in activeClients:
            wrong_decision_count += client.noteOutcome(verdicts)
        prPurple(f"\n# of clients(x)decisions who had their minds changed: {wrong_decision_count}/{len(activeClients)*len(object_locations)}")
    else:
        prPurple("\nOnly one client, no reputation changes to be made.")

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
    payload["timestamp"] = time.time()
    if client == None:
        prCyan("Attempting to create new client, "+payload["source"])
        client = initializeClient(payload["source"])
        if client == None:
            prRed("Failed to create new client")
            return
    client.setDecision(payload)
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
    elif msg.topic == "request_config":
        issueConfig()

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