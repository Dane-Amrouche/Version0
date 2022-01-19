#!/usr/bin/env python3
# THE CERCLE VERSION
import json
import socket
import threading
import time
import random

# params
FORMAT = 'utf-8'
# the commands list
JOIN =       "join"
ACCEPT =     "accept"
REJECT =     "reject"
PROTOCOL_ACK =         "ack"
PROTOCOL_GET =         "get"
PROTOCOL_ANSWER =      "answer"
PROTOCOL_PUT =         "put"
PROTOCOL_UPDATE =      "update_table"
PROTOCOL_STATS =       "stats"
PROTOCOL_PRINT =       "print"

class Node():

    # Node attributes 
    nodeIP_adress  = "127.0.0.1"  # @ip of the node instance 
    nodePort  = None  # port of the node instance 
    nodeID     = None  # id of the node instance 
    nodeData = None  # dict ofdata of nodes which it's responsible for
    nodePred =  None  # the node's predecessor
    nodeSucc =  None  # the node's successor

    # attributes for the protocole
    MAX_NODE = None  # max number of node
    BUFFER_SIZE = None  # the buffer size used in sockets communication

    # statistics attributes
    NB_GET = 0 # number of get msgs sent
    NB_PUT = 0 # number of put msgs sent
    NB_OTHERS = 0 # number of other msgs sent

    # Node initialization
    def __init__(self,dynamic,nodeID, nodeIP_adress ,nodePort, knownNode=None, max_node=256, BUFFER_SIZE = 65565):
        
        # attributes initialisation
        self.nodeIP_adress = nodeIP_adress
        self.nodePort = nodePort
        self.MAX_NODE = max_node
        self.BUFFER_SIZE = BUFFER_SIZE
       
        if dynamic:
            # get a nodeID, first pick a random ID
            id = nodeID
            # send the join command to the know node
            CMD = { 
                "cmd": JOIN,
                "args":{
                    "host":{    
                        "IP": self.nodeIP_adress,
                        "port":self.nodePort,
                        "idNode": id
                    }
                }
            }
            self.send_cmd(knownNode,CMD)
            self.NB_OTHERS +=1 # statistics
            # wait the answer
            msg = self.wait_cmd()
            print(msg["cmd"])
            if msg["cmd"]==ACCEPT :
                # complete the node insertion and search for pred and succ on the format(ip,port,id)
                self.nodeID = msg["args"]["id_requested"]
                self.nodePred = (msg["args"]["ip_port_adress_previous"]["IP"],msg["args"]["ip_port_adress_previous"]["port"],msg["args"]["ip_port_adress_previous"]["idNode"])
                self.nodeSucc = (msg["args"]["ip_port_adress_resp"]["IP"],msg["args"]["ip_port_adress_resp"]["port"],msg["args"]["ip_port_adress_resp"]["idNode"])
                self.nodeData = (msg["args"]["data"]["b1"],msg["args"]["data"]["b2"],msg["args"]["data"]["keys"])
                #send to predecessor to change his successor           
                send_CMD = {
                    "cmd":"update_table", 
                    "args":{
                        "src":{
                            "IP": self.nodeIP_adress, 
                            "port": self.nodePort, 
                            "idNode":self.nodeID
                        }, 
                        "id_lower": -1, 
                        "amount": -1
                    }
                }
                self.send_cmd((self.nodePred[0], self.nodePred[1]),send_CMD)
                self.NB_OTHERS +=1 # statistics
                #start listennig on a thread
                print("my data",self.nodeData)
                self.listen()
            else:
                print("noeud non inséré, essayer une autre clé") 
        else:
            self.nodeID = nodeID
            self.nodePred = (self.nodeIP_adress,self.nodePort,self.nodeID)
            self.nodeSucc = (self.nodeIP_adress,self.nodePort,self.nodeID)
            self.nodeData = (self.nodeID +1, self.nodeID,{})
            print("my data",self.nodeData)
            self.listen()

    def listen(self):
        #start listennig on a thread to a coming cmd
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((self.nodeIP_adress,self.nodePort))
        server.listen()
        print("[LISTENING] The server is listening on {}".format(self.nodeIP_adress,self.nodePort))

        conn, addr = server.accept()
        thread = threading.Thread(target=self.handle_Node, args=(conn,addr))
        thread.start()
        conn.close()
        server.close()
        #self.listen()               
    
    #function is_betwwen to know if a node id is between two nodes
    def is_between(self, nodeID, borne1, borne2):
        if borne1 <= borne2:
            if (nodeID >= borne1 and nodeID <= borne2) : 
                return True
            else :  
                return False
        elif borne1 == borne2:
            return False
        else :
            if (nodeID >= borne1) or (nodeID <= borne2): 
                return True
            else :
                return False

    # on receiving a join request
    # if key < nodeID i am the responsible ==> send accept
    # elqsif key == succ key ==> reject
    # else forward to succ 
    def on_join(self,CMD):

        if CMD["args"]["host"]["idNode"] == self.nodeSucc[2] or CMD["args"]["host"]["idNode"] == self.nodePred[2] or CMD["args"]["host"]["idNode"] == self.nodeID:
            send_CMD = {  
                "cmd" : REJECT, 
                "args" : { 
                    "key": CMD["args"]["host"]["idNode"]
                }
            }
            # send it the node trying to get inserted
            self.send_cmd((CMD["args"]["host"]["IP"],CMD["args"]["host"]["port"]),send_CMD)
            self.NB_OTHERS +=1 # statistics

        elif self.is_between(CMD["args"]["host"]["idNode"], self.nodePred[2], self.nodeID) or (self.nodeID==self.nodePred[2]):
            send_CMD = {  
                "cmd" : ACCEPT, 
                "args" : { 
                    "id_requested": CMD["args"]["host"]["idNode"], 
                    "ip_port_adress_resp" : {
                        "IP": self.nodeIP_adress,
                        "port":self.nodePort,
                        "idNode": self.nodeID
                    },
                    #in self.nodeData is (born1, born2, {data from born1 to borns2}) so we take data from born1 to id_requested
                    "data": {
                        "b1": self.nodeData[0],
                        "b2": CMD["args"]["host"]["idNode"],
                        "keys": dict( (key, value) for (key, value) in self.nodeData[2].items() if key <= CMD["args"]["host"]["idNode"] )
                    }, 
                    "ip_port_adress_previous": {
                        "IP": self.nodePred[0],
                        "port":self.nodePred[1],
                        "idNode": self.nodePred[2]
                    }
                }
            }
            # send it the node trying to get inserted
            self.send_cmd((CMD["args"]["host"]["IP"],CMD["args"]["host"]["port"]),send_CMD)
            self.NB_OTHERS +=1 # statistics
            # change predecessor and delete the nodes that it is not responsible anymore
            self.nodePred = (CMD["args"]["host"]["IP"],CMD["args"]["host"]["port"],CMD["args"]["host"]["idNode"])
            self.nodeData = (CMD["args"]["host"]["idNode"]+1 ,self.nodeData[1], dict( (key, value) for (key, value) in self.nodeData[2].items() if key > CMD["args"]["host"]["idNode"]))      
            if self.nodeSucc[2]== self.nodeID:
                self.nodeSucc = (CMD["args"]["host"]["IP"],CMD["args"]["host"]["port"],CMD["args"]["host"]["idNode"])

        else:
            # forward cmd to the successor
            self.send_cmd(self.nodeSucc[0:2],CMD)
            self.NB_OTHERS +=1 # statistics

    # send a msg to a node
    def send_cmd(self,node,CMD):
        try:
            # make communiction with the node
            conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
            conn.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            conn.bind((self.nodeIP_adress,self.nodePort))      
            conn.connect(node)
            print(" sent to :",node," ------------------------")
            print(json.dumps(CMD))
            print(" ------------------------")
             # send the cmd to the node
            conn.send(json.dumps(CMD).encode(FORMAT))
            conn.close()
        except (socket.error) as exc:
            # error while connecting
            print("error while connecting to node"+ str(exc))
            

    # wait the answer from responsible node
    def wait_cmd(self):
        try:
            # wait to receive an anwser
            server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind((self.nodeIP_adress,self.nodePort))
            server.listen()
            conn, addr = server.accept()
            msg = conn.recv(self.BUFFER_SIZE)
            conn.close()
            server.close()
            print(" receivedfrom: ",addr,"---------------------")
            print(msg.decode(FORMAT))
            print(" ---------------------------------")
            return json.loads(msg)
        except socket.error as exc:
            # error with server init
            print("error while connecting to node"+ str(exc))
    
    def GET_CMD(self,Dest):
        # get cmd
        CMD = { 
            "cmd": PROTOCOL_GET,
            "args":{
                "host":{ 
                    "IP": self.nodeIP_adress,
                    "port":self.nodePort,
                    "idNode":self.nodeID
                },
                "key": Dest
            }
        }
        self.on_get(CMD)

    # the node will try to get the value of Dest node
    def on_get(self, CMD):
        # check if i am the responsible
        if self.is_between(CMD["args"]["key"],self.nodePred[2]+1, self.nodeID):
            # I am the responsible so find the value in my data and send it to the dest node
            print("i am responsible for this node")
            send_CMD = { 
                "cmd": PROTOCOL_ANSWER,
                "args" : {
                     "key" : CMD["args"]["key"],
                     "value" : self.nodeData[2][CMD["args"]["key"]] if  CMD["args"]["key"] in self.nodeData[2].keys() else 0,
                     "val_exists" : CMD["args"]["key"] in self.nodeData[2].keys() 
                }
            }
            self.send_cmd((CMD["args"]["host"]["IP"],CMD["args"]["host"]["port"]),send_CMD)
            self.NB_GET +=1 # statistics
        else:
            # send the cmd to successor 
            self.send_cmd(self.nodeSucc[0:2],CMD)
            self.NB_GET +=1 # statistics
        

    # the node will try to write the value VAL in Dest node
    def PUT_CMD(self, Dest, Val):
        # get cmd
        CMD = { 
            "cmd": PROTOCOL_PUT,
            "args":{
                "host":{ 
                    "IP": self.nodeIP_adress,
                    "port":self.nodePort,
                    "idNode":self.nodeID
                },
                "key": Dest,
                "value": Val,
                "id":"put-" + str(self.nodeID)+"-"+str(self.NB_PUT)
            }
        }
        self.on_put(CMD)

    # the node will try to get the value of Dest node
    def on_put(self, CMD):
        # check if i am the responsible
        if self.is_between(CMD["args"]["key"],self.nodePred[2]+1, self.nodeID):
            # I am the responsible so find the value in my data and change it
            self.nodeData[2][CMD["args"]["key"]] = CMD["args"]["value"]
            send_CMD = { 
                "cmd": PROTOCOL_ACK,
                "args" : {
                     "id": CMD["args"]["id"]
                }
            }
            self.send_cmd((CMD["args"]["host"]["IP"],CMD["args"]["host"]["port"]),send_CMD)
            self.NB_PUT +=1 # statistics
        else:
            # send the cmd to successor 
            self.send_cmd(self.nodeSucc[0:2],CMD)
            self.NB_PUT +=1 # statistics

    def get_stats(self):
        CMD = {
            "cmd" : "stats", 
            "args" : { 
                "source":{
                    "IP": self.nodeIP_adress,
                    "port": self.nodePort,
                    "idNode": self.nodeID 
                }, 
                "nb_get": self.NB_GET, 
                "nb_put": self.NB_PUT, 
                "nb_others": self.NB_OTHERS
            }
        }
        self.send_cmd(self.nodePred[0:2],CMD)

    #handle the commands coming to this node
    def handle_Node(self,conn,addr):
        msg = json.loads(conn.recv(self.BUFFER_SIZE).decode(FORMAT))
        print(" received: ------------------------")
        print(msg)
        print("-----------------------------------")
        if msg["cmd"] == JOIN:
            self.on_join(msg)
        elif  msg["cmd"] == PROTOCOL_GET:
            self.on_get(msg)
        elif msg["cmd"] == PROTOCOL_ANSWER:
            if msg["args"]["val_exists"]:
                print("received value : " + str(msg["args"]["value"]))
            else:
                print("data does not exist")
        elif msg["cmd"] == PROTOCOL_PUT:
            self.on_put(msg)
        elif msg["cmd"] == PROTOCOL_ACK:
            #chek if the same identif then delete it fromwait queue
            # self.puts_sent.remove(msg["args"]["id"])²
            print("put msg with id "+ str(msg["args"]["id"])+" is successufly received")
        elif msg["cmd"] == PROTOCOL_UPDATE:
            # in the cercle version we only change the predecessor
            self.nodeSucc = (msg["args"]["src"]["IP"], msg["args"]["src"]["port"], msg["args"]["src"]["idNode"])
        elif msg["cmd"] == PROTOCOL_STATS:
            #if stats returns to the node that send the stats cmd the print results
            if self.nodeID == msg["args"]["source"]["idNode"]:
                print("statistics :")
                print("nomber of gets : "+ str(msg["args"]["nb_get"]))
                print("nomber of puts : "+ str(msg["args"]["nb_put"]))
                print("nomber of others : "+ str(msg["args"]["nb_others"]))
            else:
                msg["args"]["nb_get"] += self.NB_GET 
                msg["args"]["nb_put"] += self.NB_PUT 
                msg["args"]["nb_others"] += self.NB_OTHERS
                self.send_cmd((self.nodePred[0:2]),msg)

        elif msg["cmd"] == PROTOCOL_PRINT:
            print("node info : "+ str(self.nodeIP_adress)+" "+ str(self.nodePort)+" "+ str(self.nodeID))
            print("node prec : "+ str(self.nodePred))
            print("node succ : "+ str(self.nodeSucc))
            print("nodes keys that the node have from "+str(self.nodeData[0])+ " to "+str(self.nodeData[1]))
            print("data list : "+ str(self.nodeData[2]))

        elif msg["cmd"] == "send_"+PROTOCOL_GET:
            self.GET_CMD(msg["args"]["key"])

        elif msg["cmd"] == "send_"+PROTOCOL_PUT:
            self.PUT_CMD(msg["args"]["key-data"],msg["args"]["value"])

        elif msg["cmd"] == "send_"+ PROTOCOL_STATS:
            self.get_stats()

        else:
            print("Unknown command")
        self.listen()
