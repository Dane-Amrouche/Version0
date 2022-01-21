#!/usr/bin/env python3
#!/usr/bin/env python3
import json
import socket
import random
import sys
import threading
import time
import node


'''
Code partageable: 
./cmd.py get <key_data> <ip> <port>
./cmd.py put <key_data> <value> <ip> <port>
./cmd.py print <ip> <port>
./cmd.py stats <ip> <port>
'''

FORMAT = 'utf-8'
IP = socket.gethostbyname(socket.gethostname())

if sys.argv[1]=="get":
    CMD = { 
        "cmd": "send_"+node.GET,
        "args":{
            "host":{ 
                "IP": sys.argv[3],
                "port":int(sys.argv[4])
            },
            "key": int(sys.argv[2]),
        }
    }
    # the node to send data to
    known_node = (sys.argv[3],int(sys.argv[4]))

elif sys.argv[1]=="put":
    CMD = { 
        "cmd": "send_"+node.PUT,
        "args":{
            "host":{ 
                "IP": sys.argv[4],
                "port":int(sys.argv[5])
            },
            "key-data": int(sys.argv[2]),
            "value": int(sys.argv[3]),
        }
    }
    # the node to send data to
    known_node = (sys.argv[4],int(sys.argv[5])) 

elif sys.argv[1]=="stats":
    CMD = {
        "cmd" : "send_"+ node.STATS, 
        "args" : { 
            "source":{
                "IP": sys.argv[2],
                "port": int(sys.argv[3])
            }
        }
    }   
    # the node to send data to
    known_node = (sys.argv[2],int(sys.argv[3])) 

elif sys.argv[1]=="print":
    CMD = { 
        "cmd": node.PRINT
    } 
    # the node to send data to
    known_node = (sys.argv[2],int(sys.argv[3])) 
else:
    print("INVALID COMMAND")
    exit() 

#send to distination 
try:
    # make communiction with the node
    conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
    conn.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    conn.bind(("127.0.0.1",55555))      
    conn.connect(known_node)
    print(json.dumps(CMD))
    # send the cmd to the node
    conn.send(json.dumps(CMD).encode(FORMAT))
    conn.close()
except socket.error as exc:
    # error while connecting
    print("error while connecting to node"+ str(exc))



