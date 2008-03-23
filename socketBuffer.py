#    Copyright (C) 2008 David Oxley (code@psi.epsilon.org.uk)

#    This program is free software; you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation; either version 2 of the License, or
#    (at your option) any later version.

#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.

#    You should have received a copy of the GNU General Public License along
#    with this program; if not, write to the Free Software Foundation, Inc.,
#    51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

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
