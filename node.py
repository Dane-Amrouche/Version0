#!/usr/bin/env python3
# THE CERCLE VERSION
import json
import socket
import threading
import time
import random


FORMAT = 'utf-8'

# Liste des commande 
JOIN =    "join"
ACCEPT =  "accept"
REJECT =  "reject"
ACK =     "ack"
GET =     "get"
PUT =     "put"
UPDATE =  "update_table"
ANSWER =  "answer"
STATS =   "stats"
PRINT =    "print"

class Node():

    # Node attributes 
    nodeIP_adress  = "127.0.0.1" 
    nodePort  = None  
    nodeID     = None 
    nodeData = None  # donnee du noeud resp
    nodePred =  None  # noeud pred 
    nodeSucc =  None  # noeud succ

    # attributes for the protocole
    MAX_NODE = None  
    BUFFER_SIZE = None  # taille du buffer utilise dans les communication sockets 

    # variable globales pour les statistiques 
    NB_JOIN = 0 
    NB_GET = 0 
    NB_PUT = 0 


    # initialisation
    def __init__(self,notFirst,nodeID, nodeIP_adress ,nodePort, knownNode=None, max_node=65536, BUFFER_SIZE = 65565):
        
        # attributes initialisation
        self.nodeIP_adress = nodeIP_adress
        self.nodePort = nodePort
        self.MAX_NODE = max_node
        self.BUFFER_SIZE = BUFFER_SIZE
       
        if notFirst:
            # recuperer l'id du noeud, dans notre cas ca sera le num du port 
            id = nodeID
            # envoi de la commande join sous le format Json
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
            self.Send_Command(knownNode,CMD)
            self.NB_JOIN +=1 # statistics

            # attendre la reponse du noeud permettant l'acceptation ou le rejet de l'insertion
            msg = self.wait_cmd()
            print(msg["cmd"])

            if msg["cmd"]==ACCEPT :
                
                #completer l'insertion du nœud et rechercher le pred et succ en format (ip, port, id)
                # le predNew Node  est le pred du resp 
                # le succNew Node est le noeud resp de la cle 
                # NewNode sera responsable des cles pred+1 jusqu'a self.IdNode(entre deux bornes(borne 1 et borne 2))   
                self.nodeID = msg["args"]["id_requested"] # le noeud qui a demander l'insertion
                self.nodePred = (msg["args"]["info_previous_node"]["IP"],msg["args"]["info_previous_node"]["port"],msg["args"]["info_previous_node"]["idNode"])
                self.nodeSucc = (msg["args"]["info_resp_node"]["IP"],msg["args"]["info_resp_node"]["port"],msg["args"]["info_resp_node"]["idNode"])
                self.nodeData = (msg["args"]["data"]["borne1"],msg["args"]["data"]["borne2"],msg["args"]["data"]["keys"])

                # la commande a envoyer au pred pour changer son succ           
                send_CMD = {
                    "cmd":"update_table", 
                    "args":{
                        "src":{
                            "IP": self.nodeIP_adress, 
                            "port": self.nodePort, 
                            "idNode":self.nodeID % self.MAX_NODE
                        }
                    }
                }
                # envoyer un update pour le pred pour mettre a jour son succ qui sera le nouveau noeud inserer.
                self.Send_Command((self.nodePred[0], self.nodePred[1]),send_CMD)
                self.NB_JOIN +=1 # statistics
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
      
    # a la reception d'un join 
    # si key == succ or key == pred or key == self.NodeId ==> reject
    # else si key < nodeID je suis le reponsable, si key n'esxite pas ==> send accept
    # else transmettre au succ 
    def on_join(self,CMD):

        if CMD["args"]["host"]["idNode"] == self.nodeSucc[2] or CMD["args"]["host"]["idNode"] == self.nodePred[2] or CMD["args"]["host"]["idNode"] == self.nodeID:
            send_CMD = {  
                "cmd" : REJECT, 
                "args" : { 
                    "key": CMD["args"]["host"]["idNode"]
                }
            }
            # envoyer le rejet au noeud voulant s'inserer
            self.Send_Command((CMD["args"]["host"]["IP"],CMD["args"]["host"]["port"]),send_CMD)
            self.NB_JOIN +=1 # statistics

        elif self.is_between(CMD["args"]["host"]["idNode"], self.nodePred[2], self.nodeID) or (self.nodeID==self.nodePred[2]):
            send_CMD = {  
                "cmd" : ACCEPT, 
                "args" : { 
                    "id_requested": CMD["args"]["host"]["idNode"], 
                    "info_resp_node" : { # celui la va devenir son succ
                        "IP": self.nodeIP_adress,
                        "port":self.nodePort,
                        "idNode": self.nodeID
                    },
                    #in self.nodeData is (born1, born2, {data from born1 to borns2}) so we take data from born1 to id_requested
                    "data": {
                        "borne1": self.nodeData[0],
                        "borne2": CMD["args"]["host"]["idNode"],
                        "keys": dict( (key, value) for (key, value) in self.nodeData[2].items() if key <= CMD["args"]["host"]["idNode"] )
                    }, 
                    "info_previous_node": {   # mon pred va devenir son pred 
                        "IP": self.nodePred[0],
                        "port":self.nodePred[1],
                        "idNode": self.nodePred[2]
                    }
                }
            }
            # envoyer toutes les infos au noeud inserer 
            self.Send_Command((CMD["args"]["host"]["IP"],CMD["args"]["host"]["port"]),send_CMD)
            self.NB_JOIN +=1 # statistics
            # changer de prédécesseur et supprimer les nœuds dont il n'est plus responsable
            # mon pred va devenir le nouveau noeud insere 
            self.nodePred = (CMD["args"]["host"]["IP"],CMD["args"]["host"]["port"],CMD["args"]["host"]["idNode"])
            # pour mes data, je serai resp des cles NewNode+1 jusqu'a moi meme  
            self.nodeData = (CMD["args"]["host"]["idNode"]+1 ,self.nodeData[1], dict( (key, value) for (key, value) in self.nodeData[2].items() if key > CMD["args"]["host"]["idNode"]))      
            if self.nodeSucc[2]== self.nodeID:
                self.nodeSucc = (CMD["args"]["host"]["IP"],CMD["args"]["host"]["port"],CMD["args"]["host"]["idNode"])

        else:
            # transmettre au succ
            self.Send_Command(self.nodeSucc[0:2],CMD)
            self.NB_JOIN +=1 # statistics


    
    #fonction is_betwwen pour savoir si le noeud est situe entre deux bornes 
    def is_between(self, nodeID, node1, node2):
        if node1 <= node2:
            if (nodeID >= node1 and nodeID <= node2) : 
                return True
            else :  
                return False
        elif node1 == node2:
            return False
        else :
            if (nodeID >= node1) or (nodeID <= node2): 
                return True
            else :
                return False

   
    # foct pour envoyer des cmd aux noeuds 
    def Send_Command(self,node,CMD):
        try:
            # établir la communication avec le nœud
            conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
            conn.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            conn.bind((self.nodeIP_adress,self.nodePort))      
            conn.connect(node)
            print(" sent to :",node,"--------------------")
            print(json.dumps(CMD))
            print(" --------------------------------------- ")
             # envoyer cmd au noeud 
            conn.send(json.dumps(CMD).encode(FORMAT))
            conn.close()
        except (socket.error) as exc:
            print(" erreur lors de la connexion au nœud "+ str(exc))
            

    # Attendre la rep du noeud responsable 
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
            print(" received from: ",addr,"---------------------")
            print(msg.decode(FORMAT))
            print(" ---------------------------------")
            return json.loads(msg)
        except socket.error as exc:
            # error with server init
            print("erreur lors de la connexion au nœud "+ str(exc))
    
    def GET_CMD(self,Dest):
        # get cmd
        CMD = { 
            "cmd": GET,
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

#Demande des données du nœud avec la clé (key) et fournit son adresse (ip/port) pour la réponse.
    def on_get(self, CMD):
        # vérifier si je suis le responsable
        if self.is_between(CMD["args"]["key"],self.nodePred[2]+1, self.nodeID):
            print("je suis le responsable de ce noeud")
            # je suis le resp du noeud, je rep a la requete
            #Réponse à un get. Le noeud responsable répond directement à celui qui avait demandé
            #en lui redonnant la clé (key) des valeurs demandées ainsi que les valeurs (val) ou None s’il n’y a pas de valeurs pour cette clé. 
            
            send_CMD = { 
                "cmd": ANSWER,
                "args" : {
                     "key" : CMD["args"]["key"],
                     "value" : self.nodeData[2][CMD["args"]["key"]] if  CMD["args"]["key"] in self.nodeData[2].keys() else None,
                     "val_exists" : CMD["args"]["key"] in self.nodeData[2].keys() 
                }
            }

            self.Send_Command((CMD["args"]["host"]["IP"],CMD["args"]["host"]["port"]),send_CMD)
            self.NB_GET +=1 # statistics
        else:
            # send the cmd to successor 
            self.Send_Command(self.nodeSucc[0:2],CMD)
            self.NB_GET +=1 # statistics
        

    # the node will try to write the value VAL in Dest node
    def PUT_CMD(self, Dest, Val):
        # get cmd
        CMD = { 
            "cmd": PUT,
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

    def on_put(self, CMD):

        # vérifier si je suis le responsable
        if self.is_between(CMD["args"]["key"],self.nodePred[2]+1, self.nodeID):

            #Je suis le resp, trouver la valeur dans de mes données et la modifier
            self.nodeData[2][CMD["args"]["key"]] = CMD["args"]["value"]
            # Réponse à un put. Le nœud cible va répondre en direct au nœud d’origine avec l’idUniq pour lui accuser réception de son put.
            send_CMD = { 
                "cmd": ACK,
                "args" : {
                     "ok ":"ok",
                     "idUniq": CMD["args"]["id"]
                }
            }
            self.Send_Command((CMD["args"]["host"]["IP"],CMD["args"]["host"]["port"]),send_CMD)
            self.NB_PUT +=1 # statistics
        else:
            # transmettre au succ
            self.Send_Command(self.nodeSucc[0:2],CMD)
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
                "NB_PUT": self.NB_PUT, 
                "NB_JOIN": self.NB_JOIN
            }
        }
        self.Send_Command(self.nodePred[0:2],CMD)

    #handle the commands coming to this node
    def handle_Node(self,conn,addr):
        msg = json.loads(conn.recv(self.BUFFER_SIZE).decode(FORMAT))
        print(" received: ------------------------")
        print(msg)
        print("-----------------------------------")
        
        if msg["cmd"] == JOIN:
            self.on_join(msg)
        elif  msg["cmd"] == GET:
            self.on_get(msg)
        elif msg["cmd"] == ANSWER:
            if msg["args"]["val_exists"]:
                print("received value : " + str(msg["args"]["value"]))
            else:
                print("data does not exist")
        elif msg["cmd"] == PUT:
            self.on_put(msg)
        elif msg["cmd"] == ACK:
            print("put msg with id "+ str(msg["args"]["idUniq"])+" is successufly received")
        elif msg["cmd"] == UPDATE:
            # dans cette version on change uniquement le predecesseur 
            # une fois le noeud est inserer , je met a jour la TV de mon predecesseur,
            self.nodeSucc = (msg["args"]["src"]["IP"], msg["args"]["src"]["port"], msg["args"]["src"]["idNode"])
        elif msg["cmd"] == STATS:
            #if stats returns to the node that send the stats cmd the print results
            if self.nodeID == msg["args"]["source"]["idNode"]:
                print("statistics :")
                print("nombre de gets : "+ str(msg["args"]["nb_get"]))
                print("nombre de puts : "+ str(msg["args"]["NB_PUT"]))
                print("nombre de joins : "+ str(msg["args"]["NB_JOIN"]))
            else:
                msg["args"]["nb_get"] += self.NB_GET 
                msg["args"]["NB_PUT"] += self.NB_PUT 
                msg["args"]["NB_JOIN"] += self.NB_JOIN
                self.Send_Command((self.nodePred[0:2]),msg)

        elif msg["cmd"] == PRINT:
            print("node info : "+ str(self.nodeIP_adress)+" "+ str(self.nodePort)+" "+ str(self.nodeID))
            print("predecesseur : "+ str(self.nodePred))
            print("successeur : "+ str(self.nodeSucc))
            print("le noeud est resp des cles allant de "+str(self.nodeData[0])+ " jusqu'a "+str(self.nodeData[1]))
            print("liste des data: "+ str(self.nodeData[2]))

        elif msg["cmd"] == "send_"+GET:
            self.GET_CMD(msg["args"]["key"])

        elif msg["cmd"] == "send_"+PUT:
            self.PUT_CMD(msg["args"]["key-data"],msg["args"]["value"])

        elif msg["cmd"] == "send_"+ STATS:
            self.get_stats()

        else:
            print("Unknown command")
        self.listen()
