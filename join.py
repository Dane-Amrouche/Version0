#!/usr/bin/env python3
import sys
import socket
import node
import random

'''
Lancement du programme chord (comme précisé dans le sujet):
./join.py <port> - le premier qui écoute sur le port <port>
./join.py <port1> <ip> <port2> - se connecte à l’<ip> sur le <port2> et écoute sur le port1
./join.py <NodeID> <ip> <Point d'entree>
'''
#IP = socket.gethostbyname(socket.gethostname())
IP = "127.0.0.1"
NODE_ID = None
#NODE_ID= 125 random.randint(0, 255)
#print ("len %s" % len(sys.argv))
if len(sys.argv) == 2:
    #we are in the first node case
    NODE_ID = int(sys.argv[1])
    print("the choosen id",NODE_ID)
    node = node.Node(False,NODE_ID, IP,int(sys.argv[1]))
elif len(sys.argv) == 4:
    NODE_ID = int(sys.argv[1])
    print("the choosen id",NODE_ID)
    #we are in the insertion node case
    node = node.Node(True, NODE_ID,IP, int(sys.argv[1]),(sys.argv[2],int(sys.argv[3])))
else:
    # we are in invalid command
    print("INVALID PARAMETERS")
#node = node.Node(True,6,"127.0.0.1",31398,("127.0.0.1",10000))     
#print(node.nodeID)




