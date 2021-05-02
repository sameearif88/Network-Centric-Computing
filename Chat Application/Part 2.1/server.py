'''
This module defines the behaviour of server in your Chat Application
'''
import sys
import getopt
import socket
import util
import queue
from threading import Thread
import random
import time

transfer = {} #Dictionary that will store all the queues for each thread (key = address of client, value = queue for that client's thread)
clientUsername = [] #Will store the usernames of the clients
clientData = {} #Will store the address of the clients with usernames as the key
usernameExists = False #Boolian variable to check if the username is already taken or not
clientConnected = 0 #Variable to track the number of clients connected
acknowledged = False #Checks if ack is received so it can send next packet
acknowledgedAddress = () #Tuple to see which client sent the acknowledge
nextSeqNumber = 0 #Sequence number is stored in this varaible and is updated accordingly as we recieve acks

class Server:
    '''
    This is the main Server Class. You will to write Server code inside this class.
    '''
    def __init__(self, dest, port, window):
        self.server_addr = dest
        self.server_port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.settimeout(None)
        self.sock.bind((self.server_addr, self.server_port))
        self.window = window

    def process_handler(self, address):
        '''
        Takes the action necssary, according to the message received and dequeues all the chunks and reforms them
        into a single message
        '''
        global transfer
        global clientUsername
        global clientData
        global usernameExists
        global clientConnected
        sendMessage = "" #The message that will be made
        sendPacket = "" #The packet that will be send

        sendMessage = ""
        #Dequeue the chunk and concatinate until end message is received
        while True:
            item = transfer[address].get()
            #If end message is received we break the loop
            if item == "end":
                break
            #Concatinate the chunk into a varaible
            else:
                sendMessage += item

        #Recieve the message, parse it and break into list
        breakMessage = sendMessage.split()
        #Check if the client has sent the join message
        if len(breakMessage) != 0:
            if breakMessage[0].strip() == "join":
                #Check if the maximum client limit has been reached
                if clientConnected == util.MAX_NUM_CLIENTS:
                    self.send(address, "err_server_full", 2)
                    print("disconnected: server full") 
                #If the client limit has not been reached
                else:
                    #Traverse the client username list to check if the username is already taken
                    for username in clientUsername:
                        #If the username already exits
                        if breakMessage[2].strip() == username.strip():
                            self.send(address, "err_username_unavailable", 2)
                            usernameExists = True
                            print("disconnected: username not available")
                            break
                    #If the username does not exist    
                    if not usernameExists:
                        clientUsername.append(breakMessage[2])
                        clientUsername.sort(key=str.casefold)
                        clientData[breakMessage[2]] = address 
                        clientConnected += 1
                        print("join:", breakMessage[2])
                    usernameExists = False

            #Check if the client has requested the active users list
            elif breakMessage[0].strip() == "request_users_list":
                print("request_users_list:", util.get_key(clientData, address))
                listCompile = ""
                #Traverse the client username list
                for username in clientUsername:
                    listCompile += username + " "
                self.send(address, "response_users_list", 3, listCompile)

            #Check if the client wants to send the message
            elif breakMessage[0].strip() == "send_message": 
                #Check if the message does not contain a character on <numberOfClients> part
                if not breakMessage[3].isdigit():
                    self.send(address, "err_unknown_message", 2)
                    print("disconnected:", util.get_key(clientData, address))
                #Check if the number of usernames enters is not less than specified in the message
                elif len(breakMessage) - 4 <=  int(breakMessage[3]):
                    self.send(address, "err_unknown_message", 2)
                    print("disconnected:", util.get_key(clientData, address))
                #If no format error
                else:
                    print("msg:", util.get_key(clientData, address))
                    end = int(breakMessage[3]) + 4
                    #Traverse through the usernames to which client wants to send the message to
                    senderName = util.get_key(clientData, address)
                    for i in range(4, end):
                        #The destination username exits
                        if clientData.get(breakMessage[i]) is not None:
                            message = senderName + ": " + " ".join(breakMessage[end:])
                            destinationAddress = ((clientData.get(breakMessage[i]))[0], (clientData.get(breakMessage[i]))[1])
                            self.send(destinationAddress, "forward_message", 4, message)
                        #The destination username does not exits
                        else:
                            print("msg:", util.get_key(clientData, address), "to non-existent user", breakMessage[i])
                
            #Check if the client wants to send a file
            elif breakMessage[0].strip() == "send_file":
                #Check if the message does not contain a character on <numberOfClients> part
                if not breakMessage[3].isdigit():
                    self.send(address, "err_unknown_message", 2)
                    print("disconnected:", util.get_key(clientData, address))
                #If there is no format error
                else:
                    print("file:", util.get_key(clientData, address))
                    end = int(breakMessage[3]) + 4
                    #Traverse through the usernames to which client wants to send the message to
                    senderName = util.get_key(clientData, address)
                    for i in range(4, end):
                        #The destination username exits
                        if clientData.get(breakMessage[i]) is not None:
                            message = senderName + " " + " ".join(breakMessage[end:])
                            destinationAddress = ((clientData.get(breakMessage[i]))[0], (clientData.get(breakMessage[i]))[1])
                            self.send(destinationAddress, "forward_file", 4, message)
                        #The destination username does not exits
                        else:
                            print("file:", util.get_key(clientData, address), "to non-existent user", breakMessage[i])
            
            #Check if the client wants to disconnet
            elif breakMessage[0].strip() == "disconnect":
                print("disconnected:", breakMessage[2])
                deleteUsername = util.get_key(clientData, address)
                clientUsername.remove(deleteUsername)
                del clientData[deleteUsername]
                clientConnected -= 1


            #If the format recieved is something unknown
            else:
                print("disconnected:", util.get_key(clientData, address), "sent unknown command")
                self.send(address, "err_unknown_message", 2)
                deleteUsername = util.get_key(clientData, address)
                clientUsername.remove(deleteUsername)
                del clientData[deleteUsername]
                clientConnected -= 1

    def send(self, address, msgFormat, msgType, msg=None):
        '''
        Function to make message then packet 
        and then send it
        '''
        global acknowledged
        global acknowledgedAddress
        global nextSeqNumber

        sendMessage = util.make_message(msgFormat, msgType, msg)
        chunks = util.make_chunks(sendMessage)

        #Start packet
        sequenceNumber = random.randint(10, 100)
        sendPacket = util.make_packet("start", sequenceNumber,)
        self.sock.sendto(sendPacket.encode("utf-8"), address)

        #Data packets
        #For each data packet send it once acknowledge message is received
        for chunk in chunks:
            #Stop until acknowled message is not received
            while not acknowledged or acknowledgedAddress != address:
                pass
            sendPacket = util.make_packet("data", nextSeqNumber, chunk)
            self.sock.sendto(sendPacket.encode("utf-8"), address) #Send the list request message
            acknowledged = False
        
        #End packet
        #Stop until acknowled message is not received
        while not acknowledged or acknowledgedAddress != address:
            pass
        sendPacket = util.make_packet("end", nextSeqNumber,)
        self.sock.sendto(sendPacket.encode("utf-8"), address)
        acknowledged = False

    def start(self):
        '''
        Main loop.
        continue receiving messages from Clients and processing it
        '''
        global transfer
        global acknowledged
        global nextSeqNumber
        global acknowledgedAddress
        threads = {} #Dictionary for multi-threading

        #Main loop
        while True:
            receiveMessage, address = self.sock.recvfrom(4094)
            messageType, sequenceNumber, breakMessage, _ = util.parse_packet(receiveMessage.decode("utf-8"))
            sendSequence = int(sequenceNumber) + 1

            #If we get a start packet initialize a new queue for that respective client and make a thread of process_handler for it and send ack packet to client
            if messageType == "start":
                transfer[address] = queue.Queue()
                threads[address] = Thread(target=self.process_handler, args=(address,))
                threads[address].daemon = True
                threads[address].start()
                sendPacket = util.make_packet("ack", sendSequence,)
                self.sock.sendto(sendPacket.encode("utf-8"), address)

            #If we get a data packet send ack packet to client and wait for next packet
            elif messageType == "data":
                transfer[address].put(breakMessage)
                sendPacket = util.make_packet("ack", sendSequence,)
                self.sock.sendto(sendPacket.encode("utf-8"), address)

            #If we get a end packet we will delete the thread from the dictionary
            elif messageType == "end":
                transfer[address].put(messageType)
                del threads[address]

            #If we receive ack message from client we will update acknowledged and send the next packet to the client in send function
            elif messageType == "ack":
                nextSeqNumber = int(sequenceNumber)
                acknowledgedAddress = address
                acknowledged = True


# Do not change this part of code

if __name__ == "__main__":
    def helper():
        '''
        This function is just for the sake of our module completion
        '''
        print("Server")
        print("-p PORT | --port=PORT The server port, defaults to 15000")
        print("-a ADDRESS | --address=ADDRESS The server ip or hostname, defaults to localhost")
        print("-w WINDOW | --window=WINDOW The window size, default is 3")
        print("-h | --help Print this help")

    try:
        OPTS, ARGS = getopt.getopt(sys.argv[1:],
                                   "p:a:w", ["port=", "address=","window="])
    except getopt.GetoptError:
        helper()
        exit()

    PORT = 15000
    DEST = "localhost"
    WINDOW = 3

    for o, a in OPTS:
        if o in ("-p", "--port="):
            PORT = int(a)
        elif o in ("-a", "--address="):
            DEST = a
        elif o in ("-w", "--window="):
            WINDOW = a

    SERVER = Server(DEST, PORT,WINDOW)
    try:
        SERVER.start()
    except (KeyboardInterrupt, SystemExit):
        exit()