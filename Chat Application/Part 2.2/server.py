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

TRANSFER = {} #Dictionary that will store all the queues for each thread (key = address of client, value = queue for that client's thread)
CLIENTUSERNAME = [] #Will store the usernames of the clients
CLIENTDATA = {} #Will store the address of the clients with usernames as the key
USERNAMEFOUND = False #Boolian variable to check if the username is already taken or not
CLIENTCONNECTED = 0 #Variable to track the number of clients connected
ACKSTORE = {} #Dictionary to store the acks for each thread of the client (dictionary stores a queue)
EXPECTEDACK = {} #Will store the expected ack number for each thread of the client
EXPECTEDPACKET = {} #Will store the expected packet for each thread of the client
PROCEED = {} #Will make sure that we do not proceed on to converting the chunks into string message until we have the end packet

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

    def join_handler(self, breakMessage, address):
        '''
        This handles the join message from the client
        '''
        global CLIENTCONNECTED
        global USERNAMEFOUND
        global CLIENTUSERNAME
        global CLIENTDATA

        #Check if the maximum client limit has been reached
        if CLIENTCONNECTED == util.MAX_NUM_CLIENTS:
            self.send(address, "err_server_full", 2)
            print("disconnected: server full")
        #If the client limit has not been reached
        else:
            #Traverse the client username list to check if the username is already taken
            for username in CLIENTUSERNAME:
                #If the username already exits
                if breakMessage[2].strip() == username.strip():
                    self.send(address, "err_username_unavailable", 2)
                    USERNAMEFOUND = True
                    print("disconnected: username not available")
                    break
            #If the username does not exist
            if not USERNAMEFOUND:
                CLIENTUSERNAME.append(breakMessage[2])
                CLIENTUSERNAME.sort(key=str.casefold)
                CLIENTDATA[breakMessage[2]] = address
                CLIENTCONNECTED += 1
                print("join:", breakMessage[2])
            USERNAMEFOUND = False

    def list_handler(self, breakMessage, address):
        '''
        This handles the list message from the client
        '''
        global CLIENTUSERNAME

        print("request_users_list:", util.get_key(CLIENTDATA, address))
        listCompile = ""
        #Traverse the client username list
        for username in CLIENTUSERNAME:
            listCompile += username + " "
        self.send(address, "response_users_list", 3, listCompile)

    def msg_handler(self, breakMessage, address):
        '''
        This handles the message sending request from the client
        '''
        global CLIENTDATA

        #Check if the message does not contain a character on <numberOfClients> part
        if not breakMessage[3].isdigit():
            self.send(address, "err_unknown_message", 2)
            print("disconnected:", util.get_key(CLIENTDATA, address))
        #Check if the number of usernames enters is not less than specified in the message
        elif len(breakMessage) - 4 <=  int(breakMessage[3]):
            self.send(address, "err_unknown_message", 2)
            print("disconnected:", util.get_key(CLIENTDATA, address))
        #If no format error
        else:
            print("msg:", util.get_key(CLIENTDATA, address))
            end = int(breakMessage[3]) + 4
            #Traverse through the usernames to which client wants to send the message to
            senderName = util.get_key(CLIENTDATA, address)
            for i in range(4, end):
                #The destination username exits
                if CLIENTDATA.get(breakMessage[i]) is not None:
                    message = senderName + ": " + " ".join(breakMessage[end:])
                    destinationAddress = ((CLIENTDATA.get(breakMessage[i]))[0], (CLIENTDATA.get(breakMessage[i]))[1])
                    self.send(destinationAddress, "forward_message", 4, message)
                #The destination username does not exits
                else:
                    print("msg:", util.get_key(CLIENTDATA, address), "to non-existent user", breakMessage[i])

    def file_handler(self, breakMessage, address):
        '''
        This handles the file sending request from the client
        '''
        global CLIENTDATA

        #Check if the message does not contain a character on <numberOfClients> part
        if not breakMessage[3].isdigit():
            self.send(address, "err_unknown_message", 2)
            print("disconnected:", util.get_key(CLIENTDATA, address))
        #If there is no format error
        else:
            print("file:", util.get_key(CLIENTDATA, address))
            end = int(breakMessage[3]) + 4
            #Traverse through the usernames to which client wants to send the message to
            senderName = util.get_key(CLIENTDATA, address)
            for i in range(4, end):
                #The destination username exits
                if CLIENTDATA.get(breakMessage[i]) is not None:
                    message = senderName + " " + " ".join(breakMessage[end:])
                    destinationAddress = ((CLIENTDATA.get(breakMessage[i]))[0], (CLIENTDATA.get(breakMessage[i]))[1])
                    self.send(destinationAddress, "forward_file", 4, message)
                #The destination username does not exits
                else:
                    print("file:", util.get_key(CLIENTDATA, address), "to non-existent user", breakMessage[i])

    def disconnect_handler(self, breakMessage, address):
        '''
        This will handle the disconnection request from the client
        '''
        global CLIENTDATA
        global CLIENTUSERNAME
        global CLIENTCONNECTED

        #Disconnect the client and delete the username from the server data
        print("disconnected:", breakMessage[2])
        deleteUsername = util.get_key(CLIENTDATA, address)
        CLIENTUSERNAME.remove(deleteUsername)
        del CLIENTDATA[deleteUsername]
        CLIENTCONNECTED -= 1

    def unknown_handler(self, breakMessage, address):
        '''
        This handles the unknown request from the client
        '''
        global CLIENTDATA
        global CLIENTUSERNAME
        global CLIENTCONNECTED

        #Disconnect the client and remove the username from the client data
        print("disconnected:", util.get_key(CLIENTDATA, address), "sent unknown command")
        self.send(address, "err_unknown_message", 2)
        deleteUsername = util.get_key(CLIENTDATA, address)
        CLIENTUSERNAME.remove(deleteUsername)
        del CLIENTDATA[deleteUsername]
        CLIENTCONNECTED -= 1

    def parse_chunks(self, address):
        '''
        This function will convert the chunks back to a string of message
        '''
        global TRANSFER
        global PROCEED
        sendMessage = "" #The message that will be made
        item = "" #This will store each chunks one by one

        #Main loop
        while True:
            #Dont start convertion untill we recieve the confirmation that end packet has been received
            while not PROCEED[address]:
                pass
            #If the list is not empty
            if TRANSFER[address]:
                item = TRANSFER[address].pop(0)[1]
            #If end message is received we break the loop
            if item == "end":
                break
            #Concatinate the chunk into a varaible
            if item != "":
                sendMessage += item
                item = ""

        #Return the message after spliting it into list
        item = ""
        return sendMessage.split()

    def process_handler(self, address):
        '''
        Takes the action necssary, according to the message received and dequeues all the chunks and reforms them
        into a single message
        '''
        #Buid the chunks into message
        breakMessage = []
        breakMessage = self.parse_chunks(address)

        #Make sure that the list is not empty
        if len(breakMessage) != 0:
            #Check if the client has sent the join message
            if breakMessage[0].strip() == "join":
                self.join_handler(breakMessage, address)

            #Check if the client has requested the active users list
            elif breakMessage[0].strip() == "request_users_list":
                self.list_handler(breakMessage, address)

            #Check if the client wants to send the message
            elif breakMessage[0].strip() == "send_message":
                self.msg_handler(breakMessage, address)

            #Check if the client wants to send a file
            elif breakMessage[0].strip() == "send_file":
                self.file_handler(breakMessage, address)

            #Check if the client wants to disconnet
            elif breakMessage[0].strip() == "disconnect":
                self.disconnect_handler(breakMessage, address)

            #If the format recieved is something unknown
            else:
                self.unknown_handler(breakMessage, address)

    def send_window(self, address, windowStore):
        '''
        This function will send all the packets in the window
        '''
        for i in range(windowStore.qsize()):
            sendPacket = windowStore.get()
            self.sock.sendto(sendPacket.encode("utf-8"), address)
            windowStore.put(sendPacket)

    def send(self, address, msgFormat, msgType, msg=None):
        '''
        Function to make message then packet and then send it and
        maintain the window
        '''
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
        self.sock.sendto(sendPacket.encode("utf-8"), address)
        sequenceNumber += 1
        EXPECTEDACK[address] = sequenceNumber
        #Resend the start packet if no ack received
        while True:
            try:
                _ = ACKSTORE[address].get(timeout=util.TIME_OUT)
                break
            except:
                if retransmissions == util.NUM_OF_RETRANSMISSIONS:
                    break
                retransmissions += 1
                self.sock.sendto(sendPacket.encode("utf-8"), address)

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
        EXPECTEDACK[address] += 1
        self.send_window(address, windowStore)
        #Resend the data packet if no ack received and slide the window on receiving the ack
        while not windowStore.empty():
            try:
                _ = ACKSTORE[address].get(timeout=util.TIME_OUT)
                _ = windowStore.get()
                #If all the packets have been catered for we wont go in this block
                if packetIndex < len(chunks):
                    sendPacket = util.make_packet("data", sequenceNumber, chunks[packetIndex])
                    self.sock.sendto(sendPacket.encode("utf-8"), address)
                    windowStore.put(sendPacket)
                    packetIndex += 1
                    sequenceNumber += 1
                retransmissions = 0
                EXPECTEDACK[address] += 1
            except:
                if retransmissions == util.NUM_OF_RETRANSMISSIONS:
                    break
                retransmissions += 1
                self.send_window(address, windowStore)

        #End packet functionality
        sendPacket = util.make_packet("end", sequenceNumber,)
        self.sock.sendto(sendPacket.encode("utf-8"), address)
        #Resend the start packet if no ack received
        while True:
            try:
                _ = ACKSTORE[address].get(timeout=util.TIME_OUT)
                break
            except:
                if retransmissions == util.NUM_OF_RETRANSMISSIONS:
                    break
                retransmissions += 1
                self.sock.sendto(sendPacket.encode("utf-8"), address)

    def send_ack(self, address, sendSequence):
        '''
        This will send the ack packets
        '''
        sendPacket = util.make_packet("ack", sendSequence,)
        self.sock.sendto(sendPacket.encode("utf-8"), address)

    def receive_start(self, address, sendSequence, threads):
        '''
        If we receive the start packet we will implement this functionality
        '''
        global TRANSFER
        global ACKSTORE
        global EXPECTEDPACKET
        global PROCEED

        self.send_ack(address, sendSequence)
        PROCEED[address] = False
        EXPECTEDPACKET[address] = sendSequence
        ACKSTORE[address] = queue.Queue()
        TRANSFER[address] = []
        threads[address] = Thread(target=self.process_handler, args=(address,))
        threads[address].daemon = True
        threads[address].start()

    def receive_data(self, address, sendSequence, breakMessage):
        '''
        If we receive the data packet we will implement this functionality
        '''
        global TRANSFER
        global EXPECTEDPACKET

        #If the packet we recieved is not already stored in the list
        if (sendSequence -1 , breakMessage) not  in TRANSFER[address]:
            #If the packet we got is the expected packet
            if sendSequence - 1 == EXPECTEDPACKET[address]:
                TRANSFER[address].append((sendSequence - 1, breakMessage))
                TRANSFER[address].sort()
                seqNum, _ = TRANSFER[address][-1]
                EXPECTEDPACKET[address] = seqNum + 1
                self.send_ack(address, sendSequence)
            #If the packet we got is not the expected packet
            else:
                #Discard the packet if it is greater than expected packet + window size
                if sendSequence - 1 >= EXPECTEDPACKET[address] + self.window:
                    pass
                #if the packet is in range
                else:
                    TRANSFER[address].append((sendSequence - 1, breakMessage))
                    TRANSFER[address].sort()
                    self.send_ack(address, EXPECTEDPACKET[address])
        #If the packet we recieved is already stored in the list
        else:
            self.send_ack(address, EXPECTEDPACKET[address])

    def receive_end(self, address, sendSequence, threads):
        '''
        If we receive the end packet we will implement this functionality
        '''
        global TRANSFER
        global PROCEED

        self.send_ack(address, sendSequence)
        TRANSFER[address].append((sendSequence - 1, "end"))
        PROCEED[address] = True

    def receive_ack(self, address, sequenceNumber):
        '''
        If we receive the ack packet we will implement this functionality
        '''
        global ACKSTORE
        global EXPECTEDACK
        nextSeqNumber = int(sequenceNumber)
        if EXPECTEDACK[address] <= nextSeqNumber-1:
            ACKSTORE[address].put(nextSeqNumber)

    def start(self):
        '''
        Main loop.
        continue receiving messages from Clients and processing it
        '''
        threads = {} #Dictionary for multi-threading

        #Main loop
        while True:
            receiveMessage, address = self.sock.recvfrom(4096)
            messageType, sequenceNumber, breakMessage, _ = util.parse_packet(receiveMessage.decode("utf-8"))
            sendSequence = int(sequenceNumber) + 1

            #If the checksum of the received packet is valid
            if util.validate_checksum(receiveMessage.decode("utf-8")):
                #If we get a start packet initialize a new queue for that respective client and make a thread of process_handler for it and send ack packet to client
                if messageType == "start":
                    self.receive_start(address, sendSequence, threads)

                #If we get a data packet send ack packet to client and wait for next packet
                elif messageType == "data":
                    self.receive_data(address, sendSequence, breakMessage)

                #If we get a end packet we will delete the thread from the dictionary
                elif messageType == "end":
                    self.receive_end(address, sendSequence, threads)

                #If we receive ack message from client we will update acknowledged and send the next packet to the client in send function
                elif messageType == "ack":
                    self.receive_ack(address, sendSequence)


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