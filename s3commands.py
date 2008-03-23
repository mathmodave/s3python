import time
import socket
import hmac
import base64
import sha
import os
import sys
import s3parser
import socketBuffer

g_DEBUG = False;
# g_DEBUG = True;

def enableDEBUG():
	g_DEBUG = True;


def DEBUG(s):
	if (g_DEBUG):
		print s

def privateKey():
	f = open('./privatekey', 'rb')
	theKey = f.read()
	f.close()
	return theKey

def publicKey():
	f = open('./publickey', 'rb')
	theKey = f.read()
	f.close()
	return theKey

g_publicKey = publicKey()

def getSignatureREST(stringToSign):
	sigString = stringToSign.encode("UTF-8")
	hm = hmac.new(privateKey(), sigString, sha)

	return base64.b64encode(hm.digest())

def getTimeString():
	theTime = time.gmtime()
	return time.strftime('%a, %d %b %Y %H:%M:%S GMT', theTime)

def makeSocket():
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.connect(('s3.amazonaws.com', 443))
	ssl_sock = socket.ssl(s)

	return s, ssl_sock

def cleanupSocket(s, ssl_sock):
	del ssl_sock
	s.close()

def listAllBucketsReq():
	timeString = getTimeString()

	stringToSign = []
	stringToSign.append('GET')
	stringToSign.append('')
	stringToSign.append('')
	stringToSign.append(timeString)
	stringToSign.append('/')
	stringToSign = '\n'.join(stringToSign)
	
	requestString = []
	requestString.append('GET / HTTP/1.1')
	requestString.append('Host: s3.amazonaws.com')
	requestString.append('Date: %s' % timeString)
	requestString.append('Authorization: AWS %s:%s' % (g_publicKey, getSignatureREST(stringToSign)))
	requestString.append('')
	requestString.append('')
	requestString = '\n'.join(requestString)

	return requestString

def listAllBuckets():
	s, ssl_sock = makeSocket()

	req = listAllBucketsReq()
	
	DEBUG(req)

	ssl_sock.write(req)
	header, data = readResponse(ssl_sock)
	cleanupSocket(s, ssl_sock)

	return req, header, data

def putBucket(bucketName):
	s, ssl_sock = makeSocket()

	req = putBucketReq('%s' % (bucketName))
	ssl_sock.write(req)
	header, data = readResponse(ssl_sock)
	cleanupSocket(s, ssl_sock)

	return req, header, data

def putObjectFromFile(bucketName, sourceFilename, destinationFileName):
	s, ssl_sock = makeSocket()

	statInfo = os.stat(sourceFilename)

	req = putObjectReq(bucketName, destinationFileName, statInfo.st_size)
	ssl_sock.write(req)

	DEBUG(statInfo.st_size)

	fileToSocket(sourceFilename, ssl_sock)

	# response = ssl_sock.read(2048);
	header, data = readResponse(ssl_sock)
	cleanupSocket(s, ssl_sock)

	return req, header, data






def putObject(bucketName, fileName, data):
	s, ssl_sock = makeSocket()

	req, preData = putObjectReq('%s-%s' % (g_publicKey.lower(), bucketName), fileName, data)
	ssl_sock.write(req)
	time.sleep(1)
	data = ssl_sock.read(2048);
	cleanupSocket(s, ssl_sock)

	return req, data, preData

def getObject(bucketName, fileName, localTarget):
	data = ''
	s, ssl_sock = makeSocket()

	req = getObjectReq('%s' % bucketName, fileName)
	ssl_sock.write(req)
	if (localTarget != ''):
		header = readResponseToFile(ssl_sock, localTarget)
	else:
		header, data = readResponse(ssl_sock)
	cleanupSocket(s, ssl_sock)

	return req, header, data

def delObject(bucketName, fileName):
	s, ssl_sock = makeSocket()

	req = delObjectReq(bucketName, fileName)
	ssl_sock.write(req)
	header, data = readResponse(ssl_sock)
	cleanupSocket(s, ssl_sock)

	return req, header, data

def getBucket(bucketName):
	s, ssl_sock = makeSocket()

	DEBUG('in getBucket, made socket')

	req = getBucketReq(bucketName)

	DEBUG('in getBucket, made request')

	DEBUG(req)

	ssl_sock.write(req)
	header, data = readResponse(ssl_sock)
	cleanupSocket(s, ssl_sock)

	return req, header, data

def delBucket(bucketName):
	s, ssl_sock = makeSocket()

	req = delBucketReq(bucketName)
	ssl_sock.write(req)
	header, data = readResponse(ssl_sock)
	cleanupSocket(s, ssl_sock)

	return req, header, data

def putObjectReq(bucketName, fileName, fileLen):
	timeString = getTimeString()

	stringToSign = []
	stringToSign.append('PUT')
	stringToSign.append('')
	stringToSign.append('application/octet-stream')
	stringToSign.append(timeString)
	# stringToSign.append('x-amz-acl:public-read')
	stringToSign.append('/%s/%s' % (bucketName, fileName))
	stringToSign = '\n'.join(stringToSign)
	
	requestString = []
	requestString.append('PUT /%s HTTP/1.1' % fileName)
	requestString.append('Content-Type: application/octet-stream')
	# requestString.append('x-amz-acl: public-read')
	requestString.append('Content-Length: %d' % fileLen)
	requestString.append('Host: %s.s3.amazonaws.com' % bucketName)
	requestString.append('Date: %s' % timeString)
	requestString.append('Authorization: AWS %s:%s' % (g_publicKey, getSignatureREST(stringToSign)))
	requestString.append('\n')
	requestString = '\n'.join(requestString)


	return requestString

def putBucketReq(bucketName):
	timeString = getTimeString()

	stringToSign = []
	stringToSign.append('PUT')
	stringToSign.append('')
	stringToSign.append('')
	stringToSign.append(timeString)
	stringToSign.append('/%s/' % bucketName)
	stringToSign = '\n'.join(stringToSign)
	
	requestString = []
	requestString.append('PUT / HTTP/1.1')
	requestString.append('Host: %s.s3.amazonaws.com' % bucketName)
	requestString.append('Date: %s' % timeString)
	requestString.append('Authorization: AWS %s:%s' % (g_publicKey, getSignatureREST(stringToSign)))
	requestString.append('')
	requestString.append('')
	requestString = '\n'.join(requestString)

	return requestString


def getObjectReq(bucketName, fileName):
	timeString = getTimeString()

	stringToSign = []
	stringToSign.append('GET')
	stringToSign.append('')
	stringToSign.append('')
	stringToSign.append(timeString)
	stringToSign.append('/%s/%s' % (bucketName, fileName))
	stringToSign = '\n'.join(stringToSign)
	
	requestString = []
	requestString.append('GET /%s HTTP/1.1' % fileName)
	requestString.append('Host: %s.s3.amazonaws.com' % bucketName)
	requestString.append('Date: %s' % timeString)
	requestString.append('Authorization: AWS %s:%s' % (g_publicKey, getSignatureREST(stringToSign)))
	requestString.append('')
	requestString.append('')
	requestString = '\n'.join(requestString)

	return requestString

def delObjectReq(bucketName, fileName):
	timeString = getTimeString()

	stringToSign = []
	stringToSign.append('DELETE')
	stringToSign.append('')
	stringToSign.append('')
	stringToSign.append(timeString)
	stringToSign.append('/%s/%s' % (bucketName, fileName))
	stringToSign = '\n'.join(stringToSign)
	
	requestString = []
	requestString.append('DELETE /%s HTTP/1.1' % fileName)
	requestString.append('Host: %s.s3.amazonaws.com' % bucketName)
	requestString.append('Date: %s' % timeString)
	requestString.append('Authorization: AWS %s:%s' % (g_publicKey, getSignatureREST(stringToSign)))
	requestString.append('')
	requestString.append('')
	requestString = '\n'.join(requestString)

	return requestString


def getBucketReq(bucketName):
	timeString = getTimeString()

	stringToSign = []
	stringToSign.append('GET')
	stringToSign.append('')
	stringToSign.append('')
	stringToSign.append(timeString)
	stringToSign.append('/%s/' % bucketName)
	stringToSign = '\n'.join(stringToSign)
	
	requestString = []
	requestString.append('GET / HTTP/1.1')
	requestString.append('Host: %s.s3.amazonaws.com' % bucketName)
	requestString.append('Date: %s' % timeString)
	requestString.append('Authorization: AWS %s:%s' % (g_publicKey, getSignatureREST(stringToSign)))
	requestString.append('')
	requestString.append('')
	requestString = '\n'.join(requestString)

	return requestString

def delBucketReq(bucketName):
	timeString = getTimeString()

	stringToSign = []
	stringToSign.append('DELETE')
	stringToSign.append('')
	stringToSign.append('')
	stringToSign.append(timeString)
	stringToSign.append('/%s/' % bucketName)
	stringToSign = '\n'.join(stringToSign)
	
	requestString = []
	requestString.append('DELETE / HTTP/1.1')
	requestString.append('Host: %s.s3.amazonaws.com' % bucketName)
	requestString.append('Date: %s' % timeString)
	requestString.append('Authorization: AWS %s:%s' % (g_publicKey, getSignatureREST(stringToSign)))
	requestString.append('')
	requestString.append('')
	requestString = '\n'.join(requestString)

	return requestString

def readChunked(theBuf, fileTarget=None):
	chunkList = []

	while True:
		thisChunk = readChunk(theBuf, fileTarget)
		if (len(thisChunk)):
			chunkList.append(thisChunk)
		else:
			DEBUG("Got Trailer")
			CRLF = theBuf.read(2)
			if CRLF != '\r\n':
				raise Exception('No FINAL CRLF')
			else:
				DEBUG("Got final CRLF")
			break

	return ''.join(chunkList)

def readChunk(theBuf, fileTarget=None):
	chunkSize = theBuf.readTo('\n')
	chunkSize = chunkSize.rstrip('\r\n')
	chunkSize = int(chunkSize, 16)

	DEBUG("Got chunk size: %s" % chunkSize) 

	thisChunk = ''
	while (len(thisChunk) < chunkSize):
		thisChunk += theBuf.read(chunkSize - len(thisChunk))
		if (fileTarget): fileTarget.write(thisChunk)

	if (chunkSize > 0):
		CRLF = theBuf.read(2)
		if CRLF == '\r\n':
			DEBUG('Got End of Chunk CRLF')
		else:
			raise Exception('No End of chunk CRLF')
	
	return thisChunk
	

def readResponseToFile(ssl_sock, targetFile):
	"""Call when a response is expected on a socket. Function waits for the entire header, 
	then keeps reading unti the entire body has been read"""
	theBuf = socketBuffer.socketBuffer(ssl_sock)
	
	# We don't know in advance how big the header will be.
	buf = theBuf.read(1024)

	header, content_length, response = processResponse(buf)
	theBuf.putBack(response)
	response = ''

	dataWritten = 0
	f = open(targetFile, 'wb')

	DEBUG(header)
	DEBUG(content_length)

	if content_length == -1:
		# chunked
		readChunked(theBuf, f)
	else:
		while (dataWritten < content_length):
			toGo = content_length - dataWritten
			tmpBuf = theBuf.read(min(toGo, 1024))
			f.write(tmpBuf)
			dataWritten += len(tmpBuf)

	return header

def readResponse(ssl_sock):
	"""Call when a response is expected on a socket. Function waits for the entire header, 
	then keeps reading unti the entire body has been read"""
	theBuf = socketBuffer.socketBuffer(ssl_sock)
	
	# We don't know in advance how big the header will be.
	buf = theBuf.read(1024)

	header, content_length, response = processResponse(buf)
	theBuf.putBack(response)
	response = ''

	DEBUG(header)
	DEBUG(content_length)

	if content_length == -1:
		# chunked
		response = readChunked(theBuf)
	else:
		while (len(response) < content_length):
			response += theBuf.read(content_length - len(response))

	return header, response

def processResponse(response):
	header = []
	content_length = 0

	while 1:
		s = response.split('\n', 1)
		if ((s[0] == '\r') | (s[0] == '')):
			break
		else:
			header.append(s[0] + '\n')
			if (s[0].find('Content-Length')) == 0:
				content_length = s[0].split(':')[1]
			if (s[0].find('Transfer-Encoding: chunked') == 0):
				content_length = -1
			response = s[1]

	return header, int(content_length), s[1]

def fileToSocket(sourceFileName, ssl_sock):
	tot = 0
	megTot = 0
	f = open(sourceFileName, 'rb')
	
	while True:
		buf = f.read(10240)
		bufLen = len(buf)
		if bufLen == 0: break
		ssl_sock.write(buf)
		tot += bufLen
		DEBUG('written %d %d %d' % (bufLen, tot, megTot)) 
		time.sleep(0.75)
	
	f.close()

def outputHeader(h):
	for x in h:
		print x[:-2]

def doDeleteBucket(bucketName):
	req, header, data = delBucket(bucketName)

	if (header[0][:-2] != 'HTTP/1.1 204 No Content'):
		outputHeader(header)
		print ''
		print data
	else:
		print header[0][:-2] # Strip off /r/n

def doPutBucket(bucketName):
	req, header, data = putBucket(bucketName)

	if (header[0][:-2] != 'HTTP/1.1 200 OK'):
		outputHeader(header)
		print ''
		print data
	else:
		print header[0][:-2] # Strip off /r/n

def doPutObjectFromFile(bucketName, localFile, targetFile):
	req, header, data = putObjectFromFile(bucketName, localFile, targetFile)

	DEBUG(header)
	if (header[0][:-2] != 'HTTP/1.1 200 OK'):
		outputHeader(header)
		print ''
		print data
	else:
		print header[0][:-2] # Strip off /r/n

	return req, header, data

def doListAllBuckets():
	req, header, data = listAllBuckets()

	DEBUG(data)

	theParser = s3parser.listAllMyBucketsParser()
	return theParser.parse(data)

def doGetBucket(bucketName):
	req, header, data = getBucket(bucketName)

	DEBUG(req)
	DEBUG(data)

	theParser = s3parser.getBucketParser()
	return theParser.parse(data)

def doGetObject(bucketName, fileName, localTarget):
	req, header, data = getObject(bucketName, fileName, localTarget)

	DEBUG(header)
	return req, header, data

def doDeleteObject(bucketName, fileName):
	req, header, data = delObject(bucketName, fileName)

	DEBUG(header)

	return req, header, data

if __name__ == '__main__x':

	if sys.argv[1] == 'list':
		bucketList = doListAllBuckets()
		for bucket in bucketList:
			print bucket.name
			keyList = doGetBucket(bucket.name)
			for key in keyList:
				print '',
				key.dump()
	elif sys.argv[1] == 'upload':
		doPutObjectFromFile(sys.argv[2], sys.argv[3], sys.argv[4])
	elif sys.argv[1] == 'download':
		doGetObject(sys.argv[2], sys.argv[3], sys.argv[4])	
	elif sys.argv[1] == 'delete':
		doDeleteObject(sys.argv[2], sys.argv[3])
