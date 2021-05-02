'''
This module defines the behaviour of server in your Chat Application
'''
import sys
import getopt
import socket
import util


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

    def send(self, address, msgFormat, msgType, msg=None):
        '''
        Function to make message then packet 
        and then send it
        '''
        sendMessage = util.make_message(msgFormat, msgType, msg) #Make the formated message
        sendPacket = util.make_packet(msg=sendMessage) #Make the packet
        self.sock.sendto(sendPacket.encode("utf-8"), address) #Send packet

    def start(self):
        '''
        Main loop.
        continue receiving messages from Clients and processing it
        '''
        clientUsername = [] #Will store the usernames of the clients
        clientData = {} #Will store the address of the clients with usernames as the key
        usernameExists = False #Boolian variable to check if the username is already taken or not
        clientConnected = 0 #Variable to track the number of clients connected
        sendMessage = "" #The message that will be made
        sendPacket = "" #The packer that will be send
        
        #Main loop
        while True:

            #Recieve the message, parse it and break into list
            revieveMessage, address = self.sock.recvfrom(4096)
            _, _, breakMessage, _ = util.parse_packet(revieveMessage.decode("utf-8"))
            breakMessage = breakMessage.split()

            #Check if the client has sent the join message
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
                            self.send(address, "err_server_full", 2)
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
                    for i in range(4, end):
                        #The destination username exits
                        if clientData.get(breakMessage[i]) is not None:
                            message = util.get_key(clientData, address) + ": " + " ".join(breakMessage[end:])
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
                    for i in range(4, end):
                        #The destination username exits
                        if clientData.get(breakMessage[i]) is not None:
                            message = util.get_key(clientData, address) + " " + " ".join(breakMessage[end:])
                            destinationAddress = ((clientData.get(breakMessage[i]))[0], (clientData.get(breakMessage[i]))[1])
                            self.send(destinationAddress, "forward_file", 4, message)
                        #The destination username does not exits
                        else:
                            print("file:", util.get_key(clientData, address), "to non-existent user", breakMessage[i])
            
            #Check if the client wants to disconnet
            elif breakMessage[0].strip() == "disconnect":
                clientUsername.remove(breakMessage[2])
                clientData.pop(breakMessage[2])
                clientConnected -= 1
                print("disconnected:", breakMessage[2])

            #If the format recieved is something unknown
            else:
                print("disconnected:", util.get_key(clientData, address), "sent unknown command")
                self.send(address, "err_unknown_message", 2)
        
        #raise NotImplementedError


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