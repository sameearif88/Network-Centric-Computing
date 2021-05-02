'''
FILES ARE BACKED UP AT THE SUCCESSOR BECAUSE WHEN A NODE FAILS OR WHEN A NODE LEAVES WE HAVE TO TRANSFER
ALL IT'S FILES TO IT'S SUCCESSOR SO IF WE ALREADY HAVE THOSE FILES BACKED UP AT THE SUCCESSOR WE WON'T HAVE
TO RESEND THEM, REDUCING THE TIME COMPLEXITY.
'''
import socket
import threading
import os
import time
import hashlib

class Node:
	def __init__(self, host, port):
		self.stop = False
		self.host = host
		self.port = port
		self.M = 16
		self.N = 2**self.M
		self.key = self.hasher(host+str(port))
		# You will need to kill this thread when leaving, to do so just set self.stop = True
		threading.Thread(target = self.listener).start()
		self.files = []
		self.backUpFiles = []
		if not os.path.exists(host+"_"+str(port)):
			os.mkdir(host+"_"+str(port))
		'''
		------------------------------------------------------------------------------------
		DO NOT EDIT ANYTHING ABOVE THIS LINE
		'''
		# Set value of the following variables appropriately to pass Intialization test
		self.successor = (self.host, self.port)
		self.predecessor = (self.host, self.port)
		# additional state variables
		self.address = (self.host, self.port)
		self.grandSuccessor = (self.host, self.port)
		self.grandPredecessor = (self.host, self.port)
		self.joined = False
		self.getSuccess = {}
		self.inProcess = False


	def hasher(self, key):
		'''
		DO NOT EDIT THIS FUNCTION.
		You can use this function as follow:
			For a node: self.hasher(node.host+str(node.port))
			For a file: self.hasher(file)
		'''
		return int(hashlib.md5(key.encode()).hexdigest(), 16) % self.N


	def handleConnection(self, client, addr):
		'''
		 Function to handle each inbound connection, called as a thread from the listener.
		'''
		recieveMessage = (client.recv(1024)).decode("utf-8")
		message = self.parseMessage(recieveMessage, specialChar="|")

		if message[0] == "LOOK UP":
			self.lookUp(recieveMessage, message)

		elif message[0] == "1.LOOK UP RESULT":
			successorAddress = self.parseMessage(message[1], specialChar="-")
			self.successor = (successorAddress[0], int(successorAddress[1]))
			self.predecessor = self.successor
			self.joined = True

		elif message[0] == "2.LOOK UP RESULT":
			self.lookUpResult(message)
			self.joined = True

		elif message[0] == "UPDATE PREDECESSOR":
			self.updatePredecessor(message, client)

		elif message[0] == "UPDATE SUCCESSOR":
			self.updateSuccessor(message, client)

		elif message[0] == "UPDATE GRAND PREDECESSOR":
			grandPredecessorAddress = self.parseMessage(message[1], specialChar="-")
			self.grandPredecessor = (grandPredecessorAddress[0], int(grandPredecessorAddress[1]))

		elif message[0] == "UPDATE GRAND SUCCESSOR":
			grandSuccessorAddress = self.parseMessage(message[1], specialChar="-")
			self.grandSuccessor = (grandSuccessorAddress[0], int(grandSuccessorAddress[1]))

		elif message[0] == "UPDATE PREDECESSOR LEAVE":
			self.predecessor = self.grandPredecessor
			sendMessage = self.makeMessage(messageType="UPDATE GRAND PREDECESSOR", predecessorAddress=self.predecessor)
			self.sendRequest(sendMessage, self.successor)

		elif message[0] == "UPDATE SUCCESSOR LEAVE":
			self.successor = self.grandSuccessor
			sendMessage = self.makeMessage(messageType="UPDATE GRAND SUCCESSOR", successorAddress=self.successor)
			self.sendRequest(sendMessage, self.predecessor)
		
		elif message[0] == "PING":
			client.send("ALIVE".encode("utf-8"))

		elif message[0] == "PUT FILE":
			self.lookUp(recieveMessage, message, filePut=True)
		
		elif message[0] == "PUT FILE":
			self.lookUp(recieveMessage, message, filePut=True)

		elif message[0] == "PUT RESULT":
			fileName = message[1]
			address = self.extractAddress(message[2])
			self.putSend(fileName, address)

		elif message[0] == "RECEIVE PUT":
			fileName = message[1]
			self.putRecieve(client, fileName)

			sendMessage = self.makeMessage(messageType="BACKUP", myFile=fileName)
			self.sendRequest(sendMessage, self.successor)

		elif message[0] == "BACKUP":
			self.backUpFiles.append(message[1])

		elif message[0] == "GET FILE":
			self.lookUp(recieveMessage, message, fileGet=True)
		
		elif message[0] == "GET RESULT":
			fileName = message[1]
			address = self.extractAddress(message[2])
			self.getSend(fileName, address)

		elif message[0] == "RECIEVE GET":
			client.send("ACK".encode("utf-8"))
			recieveMessage = (client.recv(1024)).decode("utf-8")
			message = self.parseMessage(recieveMessage, specialChar="|")
			fileName = message[1]
			if message[0] == "FILE EXISTS":
				self.getSuccess[fileName] = True
				self.getRecive(client, fileName)
			else:
				self.getSuccess[fileName] = False

		elif message[0] == "REHASH":
			self.backUpFiles.clear()
			deleteFile = []
			senderKey = int(message[1])
			predecessorAddress = self.parseMessage(message[2], specialChar="-")
			predecessorKey = self.hasher(predecessorAddress[0] + str(predecessorAddress[1]))
			for fileName in self.files:
				fileKey = self.hasher(fileName)
				if predecessorKey < fileKey < senderKey or senderKey < predecessorKey < fileKey or fileKey < senderKey < predecessorKey:
					path = self.host + "_" + str(self.port) + "/" + fileName
					deleteFile.append(fileName)
					sendMessage = self.makeMessage(messageType="START", myFile=fileName)
					client.send(sendMessage.encode("utf-8"))
					_ = client.recv(1024)
					self.sendFile(client, path)
					_ = client.recv(1024)
			client.send("END TRANSFER".encode("utf-8"))

			for fileName in deleteFile:
				self.files.remove(fileName)
				self.backUpFiles.append(fileName)
				sendMessage = self.makeMessage(messageType="REMOVE FROM BACKUP", myFile=fileName)
				self.sendRequest(sendMessage, self.successor)
		
		elif message[0] == "REMOVE FROM BACKUP":
			self.backUpFiles.remove(message[1])

		elif message[0] == "SEND FOR BACKUP":
			address = self.extractAddress(message[1])
			for fileName in self.files:
				sendMessage = self.makeMessage(messageType="BACKUP", myFile=fileName)
				self.sendRequest(sendMessage, address)

		elif message[0] == "LEAVE FILE":
			for fileName in self.backUpFiles:
				self.files.append(fileName)
				sendMessage = self.makeMessage(messageType="BACKUP", myFile=fileName)
				self.sendRequest(sendMessage, self.successor)


	def lookUpResult(self, message):
		# Update the successor and predecessor of the new node
		self.successor = self.extractAddress(message[1])
		self.predecessor = self.extractAddress(message[2])

		# Send the predecessor of the successor
		sendMessage = self.makeMessage(messageType="UPDATE PREDECESSOR", predecessorAddress=self.address)
		soc = socket.socket()
		soc.connect(self.successor)
		soc.send(sendMessage.encode("utf-8"))
		# Update the grand successor of the new node
		recieveMessage = (soc.recv(1024)).decode("utf-8")
		recieveMessage = self.parseMessage(recieveMessage, specialChar="|")
		self.grandSuccessor = self.extractAddress(recieveMessage[1])
		# Send the grand predecessor of the new successor node
		sendMessage = self.makeMessage(messageType="YOUR GRAND PREDECESSOR", predecessorAddress=self.predecessor)
		soc.send(sendMessage.encode("utf-8"))

		# Send the successor of the predecessor
		sendMessage = self.makeMessage(messageType="UPDATE SUCCESSOR", successorAddress=self.address)
		soc = socket.socket()
		soc.connect(self.predecessor)
		soc.send(sendMessage.encode("utf-8"))
		# Update the grand predecessor of the new node
		recieveMessage = (soc.recv(1024)).decode("utf-8")
		recieveMessage = self.parseMessage(recieveMessage, specialChar="|")
		self.grandPredecessor = self.extractAddress(recieveMessage[1])
		# Send the grand predecessor of the new successor node
		sendMessage = self.makeMessage(messageType="YOUR GRAND SUCCESSOR", predecessorAddress=self.successor)
		soc.send(sendMessage.encode("utf-8"))


	def updatePredecessor(self, message, client):
		# Update the predecessor of the successor
		self.predecessor = self.extractAddress(message[1])

		# Send the grand successor of the new node
		sendMessage = self.makeMessage(messageType="YOUR GRAND SUCCESSOR", successorAddress=self.successor)
		client.send(sendMessage.encode("utf-8"))

		# Update the grand predecessor of the new node's successor
		recieveMessage = (client.recv(1024)).decode("utf-8")
		recieveMessage = self.parseMessage(recieveMessage, specialChar="|")
		self.grandPredecessor = self.extractAddress(recieveMessage[1])

		# Send the grand predecessor of the new nodes's successor's successor
		sendMessage = self.makeMessage(messageType="UPDATE GRAND PREDECESSOR", predecessorAddress=self.predecessor)
		self.sendRequest(sendMessage, self.successor)


	def updateSuccessor(self, message, client):
		# Update the successor of the predecessor
		self.successor = self.extractAddress(message[1])

		# Send the grand predecessor of the new node
		sendMessage = self.makeMessage(messageType="GRAND PREDECESSOR", predecessorAddress=self.predecessor)
		client.send(sendMessage.encode("utf-8"))

		# Update the grand successor of the new node's successor
		recieveMessage = (client.recv(1024)).decode("utf-8")
		recieveMessage = self.parseMessage(recieveMessage, specialChar="|")
		self.grandSuccessor = self.extractAddress(recieveMessage[1])

		# Send the grand successor of the new node's predecessor's predecessor
		sendMessage = self.makeMessage(messageType="UPDATE GRAND SUCCESSOR", successorAddress=self.successor)
		self.sendRequest(sendMessage, self.predecessor)


	def putSend(self, fileName, address):
		sendMessage = self.makeMessage(messageType="RECEIVE PUT", myFile=fileName)
		soc = socket.socket()
		soc.connect(address)
		soc.send(sendMessage.encode("utf-8"))
		_ = soc.recv(1024)
		self.sendFile(soc, fileName)
		_ = soc.recv(1024)


	def putRecieve(self, client, fileName):
		self.files.append(fileName)
		path = self.host + "_" + str(self.port) + "/" + fileName
		client.send("ACK".encode("utf-8"))
		self.recieveFile(client, path)
		client.send("ACK".encode("utf-8"))


	def getSend(self, fileName, address):
		sendMessage = self.makeMessage(messageType="RECIEVE GET")
		soc = socket.socket()
		soc.connect(address)
		soc.send(sendMessage.encode("utf-8"))
		_ = soc.recv(1024)

		if fileName in self.files:
			path = self.host + "_" + str(self.port) + "/" + fileName
			sendMessage = self.makeMessage(messageType="FILE EXISTS", myFile=fileName)
			soc.send(sendMessage.encode("utf-8"))
			_ = soc.recv(1024)
			self.sendFile(soc, path)
			_ = soc.recv(1024)
		else:
			sendMessage = self.makeMessage(messageType="FILE DOES NOT EXIST", myFile=fileName)
			soc.send(sendMessage.encode("utf-8"))


	def getRecive(self, client, fileName):
		path = self.host + "_" + str(self.port) + "/" + fileName
		client.send("ACK".encode("utf-8"))
		self.recieveFile(client, path)
		client.send("ACK".encode("utf-8"))


	def listener(self):
		'''
		We have already created a listener for you, any connection made by other nodes will be accepted here.
		For every inbound connection we spin a new thread in the form of handleConnection function. You do not need
		to edit this function. If needed you can edit signature of handleConnection function, but nothing more.
		'''
		listener = socket.socket()
		listener.bind((self.host, self.port))
		listener.listen(10)
		while not self.stop:
			client, addr = listener.accept()
			threading.Thread(target = self.handleConnection, args = (client, addr)).start()
		print ("Shutting down node:", self.host, self.port)
		try:
			listener.shutdown(2)
			listener.close()
		except:
			listener.close()


	def pinging(self):
		pingCount = 0
		while not self.stop:
			soc = socket.socket()
			soc.settimeout(0.1)
			try:
				soc.connect(self.successor)
				soc.send("PING".encode("utf-8"))
				_ = soc.recv(1024)
				time.sleep(0.5)
				pingCount = 0
			except:
				pingCount += 1
				if pingCount == 3:
					self.successor = self.grandSuccessor
					sendMessage = self.makeMessage(messageType="UPDATE PREDECESSOR", predecessorAddress=self.address)
					newSoc = socket.socket()
					newSoc.connect(self.successor)
					newSoc.send(sendMessage.encode("utf-8"))


					recieveMessage = (newSoc.recv(1024)).decode("utf-8")
					recieveMessage = self.parseMessage(recieveMessage, specialChar="|")
					self.grandSuccessor = self.extractAddress(recieveMessage[1])

					sendMessage = self.makeMessage(messageType="UPDATE GRAND PREDECESSOR", predecessorAddress=self.predecessor)
					newSoc.send(sendMessage.encode("utf-8"))

					sendMessage = self.makeMessage(messageType="UPDATE GRAND SUCCESSOR", successorAddress=self.successor)
					self.sendRequest(sendMessage, self.predecessor)

					sendMessage = self.makeMessage(messageType="LEAVE FILE")
					self.sendRequest(sendMessage, self.successor)
					pingCount = 0


	def join(self, joiningAddr):
		'''
		This function handles the logic of a node joining. This function should do a lot of things such as:
		Update successor, predecessor, getting files, back up files. SEE MANUAL FOR DETAILS.
		'''
		# If joining address is empty then one node case
		if joiningAddr != "":
			# Look up for the insersion successor
			sendMessage = self.makeMessage(messageType="LOOK UP", key=self.key, myAddress=self.address)
			self.sendRequest(sendMessage, joiningAddr)

		if joiningAddr != "":
			self.reHash()

		(threading.Thread(target=self.pinging)).start()


	def reHash(self):
		transfering = True
		while not self.joined:
			pass
		sendMessage = self.makeMessage(messageType="REHASH", key=self.key, predecessorAddress=self.predecessor)
		soc = socket.socket()
		soc.connect(self.successor)
		soc.send(sendMessage.encode("utf-8"))
		while transfering:
			recieveMessage = (soc.recv(1024)).decode("utf-8")
			message = self.parseMessage(recieveMessage, specialChar="|")
			if message[0] == "END TRANSFER":
				transfering = False
			else:
				fileName = message[1]
				self.files.append(fileName)
				path = self.host + "_" + str(self.port) + "/" + fileName
				soc.send("ACK".encode("utf-8"))
				self.recieveFile(soc, path)
				soc.send("ACK".encode("utf-8"))

		sendMessage = self.makeMessage(messageType="SEND FOR BACKUP", myAddress=self.address)
		self.sendRequest(sendMessage, self.predecessor)


	def lookUp(self, recieveMessage, message, filePut=False, fileGet=False):
		# If we are looking for a place for a node
		if not filePut and not fileGet:
			findKey = int(message[1])
			successorKey = self.hasher(self.successor[0] + str(self.successor[1]))
			# We know the successor of the new node
			if self.key < findKey < successorKey or successorKey < self.key < findKey or findKey < successorKey < self.key:
				sendMessage = self.makeMessage(messageType="2.LOOK UP RESULT", successorAddress=self.successor, predecessorAddress=self.address)
				address = self.extractAddress(message[2])
				self.sendRequest(sendMessage, address)
			# If there are only two nodes
			elif self.successor == self.address:
				sendMessage = self.makeMessage(messageType="1.LOOK UP RESULT", successorAddress=self.address)
				address = self.extractAddress(message[2])
				self.successor = address
				self.predecessor = address
				self.sendRequest(sendMessage, address)
			# Forward the look up query
			else:
				self.sendRequest(recieveMessage, self.successor)
		# If we are looking for a place to put file
		elif filePut:
			fileKey = self.hasher(message[1])
			successorKey = self.hasher(self.successor[0] + str(self.successor[1]))
			# We know the successor at which the file should be places
			if self.key < fileKey < successorKey or successorKey < self.key < fileKey or fileKey < successorKey < self.key:
				sendMessage = self.makeMessage(messageType="PUT RESULT", myFile=message[1], successorAddress=self.successor)
				address = self.extractAddress(message[2])
				self.sendRequest(sendMessage, address)
			# Forward the look up query
			else:
				self.sendRequest(recieveMessage, self.successor)
		# If we are looking for the place the file is at
		elif fileGet:
			fileKey = self.hasher(message[1])
			successorKey = self.hasher(self.successor[0] + str(self.successor[1]))
			# We know the successor at which the file is
			if self.key < fileKey < successorKey or successorKey < self.key < fileKey or fileKey < successorKey < self.key:
				address = self.extractAddress(message[2])
				sendMessage = self.makeMessage(messageType="GET RESULT", myFile=message[1], myAddress=address)
				self.sendRequest(sendMessage, self.successor)
			# Forward the look up query
			else:
				self.sendRequest(recieveMessage, self.successor)


	def put(self, fileName):
		'''
		This function should first find node responsible for the file given by fileName, then send the file over the socket to that node
		Responsible node should then replicate the file on appropriate node. SEE MANUAL FOR DETAILS. Responsible node should save the files
		in directory given by host_port e.g. "localhost_20007/file.py".
		'''
		sendMessage = self.makeMessage(messageType="PUT FILE", myFile=fileName, myAddress=self.address)
		self.sendRequest(sendMessage, self.address)


	def get(self, fileName):
		'''
		This function finds node responsible for file given by fileName, gets the file from responsible node, saves it in current directory
		i.e. "./file.py" and returns the name of file. If the file is not present on the network, return None.
		'''
		while self.inProcess:
			pass
		self.inProcess = True
		self.getSuccess[fileName] = None
		sendMessage = self.makeMessage(messageType="GET FILE", myFile=fileName, myAddress=self.address)
		self.sendRequest(sendMessage, self.address)

		while self.getSuccess[fileName] is None:
			pass
		if self.getSuccess[fileName]:
			del self.getSuccess[fileName]
			self.inProcess = False
			return fileName
		else:
			del self.getSuccess[fileName]
			self.inProcess = False
			return None


	def leave(self):
		'''
		When called leave, a node should gracefully leave the network i.e. it should update its predecessor that it is leaving
		it should send its share of file to the new responsible node, close all the threads and leave. You can close listener thread
		by setting self.stop flag to True
		'''
		sendMessage = self.makeMessage(messageType="UPDATE PREDECESSOR LEAVE")
		self.sendRequest(sendMessage, self.successor)

		sendMessage = self.makeMessage(messageType="UPDATE SUCCESSOR LEAVE")
		self.sendRequest(sendMessage, self.predecessor)

		sendMessage = self.makeMessage(messageType="UPDATE GRAND PREDECESSOR", predecessorAddress=self.grandPredecessor)
		self.sendRequest(sendMessage, self.successor)

		sendMessage = self.makeMessage(messageType="UPDATE GRAND SUCCESSOR", successorAddress=self.grandSuccessor)
		self.sendRequest(sendMessage, self.predecessor)

		sendMessage = self.makeMessage(messageType="LEAVE FILE")
		self.sendRequest(sendMessage, self.successor)

		for fileName in self.backUpFiles:
			sendMessage = self.makeMessage(messageType="BACKUP", myFile=fileName)
			self.sendRequest(sendMessage, self.successor)

		self.kill()


	def sendFile(self, soc, fileName):
		'''
		Utility function to send a file over a socket
			Arguments:	soc => a socket object
						fileName => file's name including its path e.g. NetCen/PA3/file.py
		'''
		fileSize = os.path.getsize(fileName)
		soc.send(str(fileSize).encode('utf-8'))
		soc.recv(1024).decode('utf-8')
		with open(fileName, "rb") as file:
			contentChunk = file.read(1024)
			while contentChunk!="".encode('utf-8'):
				soc.send(contentChunk)
				contentChunk = file.read(1024)


	def recieveFile(self, soc, fileName):
		'''
		Utility function to recieve a file over a socket
			Arguments:	soc => a socket object
						fileName => file's name including its path e.g. NetCen/PA3/file.py
		'''
		fileSize = int(soc.recv(1024).decode('utf-8'))
		soc.send("ok".encode('utf-8'))
		contentRecieved = 0
		file = open(fileName, "wb")
		while contentRecieved < fileSize:
			contentChunk = soc.recv(1024)
			contentRecieved += len(contentChunk)
			file.write(contentChunk)
		file.close()


	def kill(self):
		# DO NOT EDIT THIS, used for code testing
		self.stop = True


	def makeMessage(self, messageType="", myFile="", key=None, myAddress=(), successorAddress=(), predecessorAddress=()):
		messageList = []
		if myFile != "":
			messageList.append(myFile)
		if key != None:
			messageList.append(str(key))
		if myAddress != ():
			myAddr = str(myAddress[0]) + "-" + str(myAddress[1])
			messageList.append(myAddr)
		if successorAddress != ():
			succAddr = str(successorAddress[0]) + "-" + str(successorAddress[1])
			messageList.append(succAddr)
		if predecessorAddress != ():
			predAddr = str(predecessorAddress[0]) + "-" + str(predecessorAddress[1])
			messageList.append(predAddr)
		message = messageType
		for item in messageList:
			message = message + "|" + item
		return message


	def parseMessage(self, message, specialChar=""):
		return message.split(specialChar)


	def extractAddress(self, address):
		address = self.parseMessage(address, specialChar="-")
		address = (address[0], int(address[1]))
		return address


	def sendRequest(self, sendMessage, address):
		soc = socket.socket()
		soc.connect(address)
		soc.send(sendMessage.encode("utf-8"))