'''
This module defines the behaviour of a client in your Chat Application
'''
import sys
import getopt
import socket
import random
from threading import Thread
import os
import util


'''
Write your code inside this class. 
In the start() function, you will read user-input and act accordingly.
receive_handler() function is running another thread and you have to listen 
for incoming messages in this function.
'''
stop = False

class Client:
    '''
    This is the main Client Class. 
    '''
    def __init__(self, username, dest, port, window_size):
        self.server_addr = dest
        self.server_port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.settimeout(None)
        self.sock.bind(('localhost', random.randint(10000, 40000)))
        self.name = username
        self.window = window_size

    def send(self, msgFormat, msgType, msg=None):
        '''
        Function to make message then packet 
        and then send it
        '''
        sendMessage = util.make_message(msgFormat, msgType, msg) #Make the message
        sendPacket = util.make_packet(msg=sendMessage) #Make the packet
        self.sock.sendto(sendPacket.encode("utf-8"), (self.server_addr, self.server_port)) #Send the list request message


    def start(self):
        '''
        Main Loop is here
        Start by sending the server a JOIN message.
        Waits for userinput and then process it
        '''
        global stop
        self.send("join", 1, self.name)

        #Main loop
        while not stop:

            sendMessage = input()

            #If the user wants to request list of active clients
            if sendMessage.strip() == "list": 
                self.send("request_users_list", 2)

            #If the user wants to send the message
            elif sendMessage.split()[0] == "msg":
                self.send("send_message", 4, sendMessage)
            
            #If the user wants to send a file
            elif sendMessage.split()[0] == "file":
                fileName = sendMessage.split()[len(sendMessage.split()) - 1]
                fileHandle = open(fileName)
                fileData = fileHandle.read()
                fileHandle.close()
                sendMessage = sendMessage + " " + fileData
                self.send("send_file", 4, sendMessage)
            
            #The help function
            elif sendMessage.strip() == "help":
                print("User Inputs and Formats:")
                print("Available Users: list")
                print("Message: msg <number_of_users> <username1> <username2> ... <message>")
                print("File Sharing: file <number_of_users> <username1> <username2> ... <file_name>")
                print("Quit: quit")

            
            #If the user wants to disconnect
            elif sendMessage.split()[0] == "quit":
                self.send("disconnect", 1, self.name)
                print("quitting")
                stop = True
                self.sock.close()
            
            #If the format of the input message is not recognised
            else:
                print("incorrect userinput format")          

        #raise NotImplementedError
            

    def receive_handler(self):
        '''
        Waits for a message from server and process it accordingly
        '''
        global stop #To stop the main loop

        #Main loop
        while not stop:
            revieveMessage, address = self.sock.recvfrom(self.server_port)
            _, _, breakMessage, _ = util.parse_packet(revieveMessage.decode("utf-8"))
            breakMessage = breakMessage.split()

            #If the clients request to join has been rejected
            if breakMessage[0].strip() == "join":
                print(breakMessage[2])

            #If the clients request for active user list has been processed
            elif breakMessage[0].strip() == "response_users_list":
                print("list:", " ".join(breakMessage[2:]))
            
            #If someone has send a message
            elif breakMessage[0].strip() == "forward_message":
                print("msg:", " ".join(breakMessage[2:]))

            #If someone has send a file
            elif breakMessage[0].strip() == "forward_file":
                fileName = breakMessage[3]
                fileData = " ".join(breakMessage[4:])
                print("file:", breakMessage[2] + ":", fileName)
                fileName = self.name + "_" + fileName
                fileHandle = open(fileName, "w")
                fileHandle.write(fileData)
                fileHandle.close()

            #If the server is full
            elif breakMessage[0].strip() == "err_server_full":
                print("disconnected: server full")
                stop = True
                self.sock.close()

            #If the username is already taken
            elif breakMessage[0].strip() == "err_username_unavailable":
                print("disconnected: username not available")
                stop = True
                self.sock.close()

            #If the client send an unknown message format
            elif breakMessage[0].strip() == "err_unknown_message":
                print("disconnected: server received an unknown command")
                stop = True
                self.sock.close()
                
        #raise NotImplementedError



# Do not change this part of code
if __name__ == "__main__":
    def helper():
        '''
        This function is just for the sake of our Client module completion
        '''
        print("Client")
        print("-u username | --user=username The username of Client")
        print("-p PORT | --port=PORT The server port, defaults to 15000")
        print("-a ADDRESS | --address=ADDRESS The server ip or hostname, defaults to localhost")
        print("-w WINDOW_SIZE | --window=WINDOW_SIZE The window_size, defaults to 3")
        print("-h | --help Print this help")
    try:
        OPTS, ARGS = getopt.getopt(sys.argv[1:],
                                   "u:p:a:w", ["user=", "port=", "address=","window="])
    except getopt.error:
        helper()
        exit(1)

    PORT = 15000
    DEST = "localhost"
    USER_NAME = None
    WINDOW_SIZE = 3
    for o, a in OPTS:
        if o in ("-u", "--user="):
            USER_NAME = a
        elif o in ("-p", "--port="):
            PORT = int(a)
        elif o in ("-a", "--address="):
            DEST = a
        elif o in ("-w", "--window="):
            WINDOW_SIZE = a

    if USER_NAME is None:
        print("Missing Username.")
        helper()
        exit(1)

    S = Client(USER_NAME, DEST, PORT, WINDOW_SIZE)
    try:
        # Start receiving Messages
        T = Thread(target=S.receive_handler)
        T.daemon = True
        T.start()
        # Start Client
        S.start()
    except (KeyboardInterrupt, SystemExit):
        sys.exit()
