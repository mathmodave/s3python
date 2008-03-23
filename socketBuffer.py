import socket

class socketBuffer(object):
	def __init__(self, ssl_sock):
		self._ssl_sock = ssl_sock
		self._buf = ''

	def read(self, numBytes):
		if (numBytes == 0):
			return ''
		buf = self._buf
		ret = ''
		if (numBytes <= len(buf)):
			ret = buf[:numBytes]
			buf = buf[numBytes:]
		else:
			buf += self._ssl_sock.read(numBytes - len(buf)) 
			ret = buf
			buf = ''
		
		self._buf = buf
		return ret

	def putBack(self, buf):
		self._buf = buf + self._buf

	
	def readTo(self, target):
		buf = self._buf

		posn = buf.find(target)
		if (posn == -1):
			buf = self.read(1024)
			self.putBack(buf)
			return self.readTo(target)
		else:
			posn += len(target)
			ret = buf[:posn]
			self._buf = buf[posn:]
			return ret
