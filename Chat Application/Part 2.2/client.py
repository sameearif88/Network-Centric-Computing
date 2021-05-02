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
import time
import queue


'''
Write your code inside this class.
In the start() function, you will read user-input and act accordingly.
receive_handler() function is running another thread and you have to listen
for incoming messages in this function.
'''
STOP = False #Will STOP the client thread
ACKNOWLEDGED = False #Checks if ack is received so it can send next packet
ACKSTORE = queue.Queue() #queue to store the acks for client
EXPECTEDACK = 0 #Will store the expected ack number client
EXPECTEDPACKET = 0 #Will store the expected packet for client

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

    def send_window(self, windowStore):
        '''
        This function will send all the packets in the window
        '''
        for _ in range(windowStore.qsize()):
            sendPacket = windowStore.get()
            self.sock.sendto(sendPacket.encode("utf-8"), (self.server_addr, self.server_port))
            windowStore.put(sendPacket)

    def send(self, msgFormat, msgType, msg=None):
        '''
        Function to make message then packet and then send it and
        maintain the window
        '''
        global STOP
        global ACKSTORE
        global EXPECTEDACK
        windowStore = queue.Queue()
        retransmissions = 0
        packetIndex = 0

        sendMessage = util.make_message(msgFormat, msgType, msg)
        chunks = util.make_chunks(sendMessage)

        #Start packet functionality
        sequenceNumber = random.randint(10, 100)
        sendPacket = util.make_packet("start", sequenceNumber,)
        self.sock.sendto(sendPacket.encode("utf-8"), (self.server_addr, self.server_port))
        sequenceNumber += 1
        EXPECTEDACK = sequenceNumber
        #Resend the start packet if no ack received
        while True:
            try:
                _ = ACKSTORE.get(timeout=util.TIME_OUT)
                retransmissions = 0
                break
            except:
                if retransmissions == util.NUM_OF_RETRANSMISSIONS:
                    break
                retransmissions += 1
                self.sock.sendto(sendPacket.encode("utf-8"), (self.server_addr, self.server_port))

        #Data packets functionality
        #Store the packets in the queue (number of packets stored <= window size)
        for i in range(self.window):
            sendPacket = util.make_packet("data", sequenceNumber, chunks[i])
            windowStore.put(sendPacket)
            sequenceNumber += 1
            #If packets required to send < window size
            if i == len(chunks) - 1:
                break
        packetIndex = self.window
        EXPECTEDACK += 1
        self.send_window(windowStore)
        #Resend the data packet if no ack received and slide the window on receiving the ack
        while not windowStore.empty():
            try:
                _ = ACKSTORE.get(timeout=util.TIME_OUT)
                _ = windowStore.get()
                #If all the packets have been catered for we wont go in this block
                if packetIndex < len(chunks):
                    sendPacket = util.make_packet("data", sequenceNumber, chunks[packetIndex])
                    self.sock.sendto(sendPacket.encode("utf-8"), (self.server_addr, self.server_port))
                    windowStore.put(sendPacket)
                    packetIndex += 1
                    sequenceNumber += 1
                retransmissions = 0
                EXPECTEDACK += 1
            except:
                if retransmissions == util.NUM_OF_RETRANSMISSIONS:
                    break
                retransmissions += 1
                self.send_window(windowStore)

        #End packet functionality
        sendPacket = util.make_packet("end", sequenceNumber,)
        self.sock.sendto(sendPacket.encode("utf-8"), (self.server_addr, self.server_port))
        #Resend the start packet if no ack received
        while True:
            try:
                checkAck = ACKSTORE.get(timeout=util.TIME_OUT)
                # if checkAck == EXPECTEDACK:
                retransmissions = 0
                break
            except:
                if retransmissions == util.NUM_OF_RETRANSMISSIONS:
                    break
                retransmissions += 1
                self.sock.sendto(sendPacket.encode("utf-8"), (self.server_addr, self.server_port))

        #Close the socket if the message was to disconnect
        if msgFormat == "disconnect":
            print("quitting")
            self.sock.close()
            STOP = True

    def list_handler(self):
        '''
        If the clients wants to request the list of users
        '''
        self.send("request_users_list", 2)

    def msg_handler(self, sendMessage):
        '''
        If the clients wants to send the message
        '''
        self.send("send_message", 4, sendMessage)

    def file_handler(self, sendMessage):
        '''
        If the clients wants to send the file
        '''
        fileName = sendMessage.split()[len(sendMessage.split()) - 1]
        fileHandle = open(fileName)
        fileData = fileHandle.read()
        fileHandle.close()
        sendMessage = sendMessage + " " + fileData
        self.send("send_file", 4, sendMessage)

    def help_handler(self):
        '''
        If the clients want informations about the functionality
        '''
        print("User Inputs and Formats:")
        print("Available Users: list")
        print("Message: msg <number_of_users> <username1> <username2> ... <message>")
        print("File Sharing: file <number_of_users> <username1> <username2> ... <file_name>")
        print("Quit: quit")

    def disconnect_handler(self):
        '''
        If the clients wants to disconnect
        '''
        self.send("disconnect", 1, self.name)

    def unknown_handler(self):
        '''
        If the clients inputs an unknown command
        '''
        print("incorrect userinput format")

    def start(self):
        '''
        Main Loop is here
        Start by sending the server a JOIN message.
        Waits for userinput and then process it
        '''
        self.send("join", 1, self.name)

        #Main loop
        while not STOP:

            sendMessage = input()

            #If the user wants to request list of active clients
            if sendMessage.strip() == "list":
                self.list_handler()

            #If the user wants to send the message
            elif sendMessage.split()[0] == "msg":
                self.msg_handler(sendMessage)

            #If the user wants to send a file
            elif sendMessage.split()[0] == "file":
                self.file_handler(sendMessage)

            #The help function
            elif sendMessage.strip() == "help":
                self.help_handler()

            #If the user wants to disconnect
            elif sendMessage.split()[0] == "quit":
                self.disconnect_handler()

            #If the format of the input message is not recognised
            else:
                self.unknown_handler()

    def receive_ack(self, sequenceNumber):
        '''
        If we receive the ack packet we will implement this functionality
        '''
        global ACKSTORE
        global EXPECTEDACK
        nextSeqNumber = int(sequenceNumber)
        print("expected: ", EXPECTEDACK)

        if EXPECTEDACK <= nextSeqNumber:
            print("EQUAL")
            ACKSTORE.put(nextSeqNumber)

    def receive_start(self, sequenceNumber, chunks):
        '''
        If we receive the start packet we will implement this functionality
        '''
        global EXPECTEDPACKET

        EXPECTEDPACKET = int(sequenceNumber) + 1
        sendPacket = util.make_packet("ack", EXPECTEDPACKET,)
        self.sock.sendto(sendPacket.encode("utf-8"), (self.server_addr, self.server_port))

    def receive_data(self, sequenceNumber, chunks, message):
        '''
        If we receive the data packet we will implement this functionality
        '''
        global EXPECTEDPACKET

        sequenceNumber = int(sequenceNumber)
        #If the packet we recieved is not already stored in the list
        if (sequenceNumber, message) not  in chunks:
            #If the packet we got is the expected packet
            if sequenceNumber == EXPECTEDPACKET:
                chunks.append((sequenceNumber, message))
                chunks.sort()
                s, _ = chunks[-1]
                EXPECTEDPACKET = s +1
                sendPacket = util.make_packet("ack", sequenceNumber + 1,)
                self.sock.sendto(sendPacket.encode("utf-8"), (self.server_addr, self.server_port))
            #If the packet we got is not the expected packet
            else:
                #Discard the packet if it is greater than expected packet + window size
                if sequenceNumber >= EXPECTEDPACKET + self.window:
                    pass
                #if the packet is in range
                else:
                    chunks.append((sequenceNumber, message))
                    chunks.sort()
                    sendPacket = util.make_packet("ack", EXPECTEDPACKET,)
                    self.sock.sendto(sendPacket.encode("utf-8"), (self.server_addr, self.server_port))
        #If the packet we recieved is already stored in the list
        else:
            sendPacket = util.make_packet("ack",EXPECTEDPACKET,)
            self.sock.sendto(sendPacket.encode("utf-8"), (self.server_addr, self.server_port))

    def receive_end(self, sequenceNumber, chunks):
        '''
        If we receive the end packet we will implement this functionality
        '''

        sendPacket = util.make_packet("ack", int(sequenceNumber) + 1,)
        self.sock.sendto(sendPacket.encode("utf-8"), (self.server_addr, self.server_port))
        extractMessage = ""
        #For each chunk recieved concatinate it into message
        for chunk in chunks:
            extractMessage += chunk[1]
        return extractMessage.split()

    def receive_handler(self):
        '''
        Waits for a message from server and process it accordingly
        '''
        global STOP
        chunks = []

        #Main loop
        while not STOP:
            receiveMessage, _ = self.sock.recvfrom(self.server_port)
            messageType, sequenceNumber, message, _ = util.parse_packet(receiveMessage.decode("utf-8"))

            #If the checksum of the received packet is valid
            if util.validate_checksum(receiveMessage.decode("utf-8")):
                #If client recieved the ack packet it will update ACKNOWLEDGED and send the next packet using the send function
                if messageType == "ack":
                    self.receive_ack(sequenceNumber)

                #If client recieves the start packet then it will send ack packet and wait for next packet from the server
                elif messageType == "start":
                    self.receive_start(sequenceNumber, chunks)

                #If client recieves the data packet it will send ack packet and wait for the next packet from the server
                elif messageType == "data":
                    self.receive_data(sequenceNumber, chunks, message)

                #If client recieves the end packet it will join all the chunks together and process the message
                elif messageType == "end":
                    breakMessage = []
                    #For each chunk recieved concatinate it into message
                    breakMessage = self.receive_end(sequenceNumber, chunks)

                    #If the list is not empty
                    if breakMessage:
                        #If clients asks to join the server
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
                            STOP = True
                            self.sock.close()

                        #If the username is already taken
                        elif breakMessage[0].strip() == "err_username_unavailable":
                            print("disconnected: username not available")
                            STOP = True
                            self.sock.close()

                        #If the client send an unknown message format
                        elif breakMessage[0].strip() == "err_unknown_message":
                            print("disconnected: server received an unknown command")
                            STOP = True
                            self.sock.close()

                        chunks = []


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
