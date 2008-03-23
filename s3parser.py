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

import xml.parsers.expat

class grant(object):
	def __init__(self):
		self._xmlString = ''
		self._displayName = ''
		self._URI = ''
		self._publicRead = False
	
	def _getString(self):
		return self._xmlString
	
	def _setString(self, newString):
		self._xmlString = newString
	
	xmlString = property(_getString, _setString)

	def _getDisplayName(self):
		return self._displayName

	def _setDisplayName(self, newDisplayName):
		self._displayName = newDisplayName

	displayName = property(_getDisplayName, _setDisplayName)

	def _getURI(self):
		return self._URI

	def _setURI(self, newURI):
		self._URI = newURI

	URI = property(_getURI, _setURI)

	def _getPublicRead(self):
		return self._publicRead
	
	def _setPublicRead(self, newValue):
		self._publicRead = newValue

	publicRead = property(_getPublicRead, _setPublicRead)


class bucket(object):
	def __init__(self):
		self._name = ''
		self._creationDate = ''

	def getName(self):
		return self._name
	
	def setName(self, newName):
		self._name = newName
	
	name = property(getName, setName)

	def getCreationDate(self):
		return self._creationDate
	
	def setCreationDate(self, newDate):
		self._creationDate = newDate
	
	creationDate = property(getCreationDate, setCreationDate)

	def dump(self):
		print self._name, self._creationDate

class s3File(object):
	def __init__(self):
		self._key = ''
		self._md5 = ''
		self._lastModified = ''
		self._size = 0

	def getKey(self):
		return self._key
	
	def setKey(self, newValue):
		self._key = newValue
	
	def getMD5(self):
		return self._md5

	def setMD5(self, newValue):
		self._md5 = newValue
	
	def getLastModified(self):
		return self._lastModified
	
	def setLastModified(self, newValue):
		self._lastModified = newValue

	def getSize(self):
		return self._size
	
	def setSize(self, newValue):
		self._size = newValue

	key = property(getKey, setKey)
	size = property(getSize, setSize)
	md5 = property(getMD5, setMD5)
	lastModified = property(getLastModified, setLastModified)

	def dump(self):
		print self._key, self._lastModified, self._md5, self._size

class parser(object):
	def __init__(self):
		self._p = xml.parsers.expat.ParserCreate()
	
	def parse(self, data):
		self._p.Parse(data)

class listAllMyBucketsParser(object):
	def __init__(self):
		self._p = xml.parsers.expat.ParserCreate()
		self._p.StartElementHandler = self._start_element
		self._p.EndElementHandler = self._end_element
		self._p.CharacterDataHandler = self._char_data
		self._curBucket = None
		self._inName = False
		self._inCreationDate = False

		self._bucketList = []

	def parse(self, data):
		self._p.Parse(data)
		return self._bucketList

	def _start_element(self, name, attrs):
		if name == 'Bucket':
			self._curBucket = bucket()
		elif name == 'CreationDate':
			self._inCreationDate = True
		elif name == 'Name':
			self._inName = True

	def _char_data(self, data):
		if self._inCreationDate:
			self._curBucket.creationDate = data
		elif self._inName:
			self._curBucket.name = data

	def _end_element(self, name):
		if name == 'Bucket':
			# self._curBucket.dump()
			self._bucketList.append(self._curBucket)
			self._curBucket = None
		elif name == 'CreationDate':
			self._inCreationDate = False
		elif name == 'Name':
			self._inName = False


class getBucketParser(object):
	def __init__(self):
		self._p = xml.parsers.expat.ParserCreate()
		self._p.StartElementHandler = self._start_element
		self._p.EndElementHandler = self._end_element
		self._p.CharacterDataHandler = self._char_data
	
		self._curFile = None
		self._fileList = []
		self._eltStack = []

	def parse(self, data):
		self._p.Parse(data)
		return self._fileList

	def _start_element(self, name, attrs):
		self._eltStack.append(name)

		if name == 'Key':
			self._curFile = s3File()

	def _char_data(self, data):
		curElt = self._eltStack[-1]

		if curElt == 'Key':
			self._curFile.key = data
		elif curElt == 'LastModified':
			self._curFile.lastModified = data
		elif curElt == 'ETag':
			if len(data) > 5:
				self._curFile.md5 = data
		elif curElt == 'Size':
			self._curFile.size = data

	def _end_element(self, data):
		curElt = self._eltStack.pop()
		assert(curElt == data)
	
		if curElt != data:
			print 'Stack Mismatch: %s %s' % (curElt, data)

		if curElt == 'Contents':
			# self._curFile.dump()
			self._fileList.append(self._curFile)
			self._curFile = None


class aclParser(object):
	def __init__(self):
		self._p = xml.parsers.expat.ParserCreate()
		self._p.StartElementHandler = self._start_element
		self._p.EndElementHandler = self._end_element
		self._p.CharacterDataHandler = self._char_data

		self._grantList = []
		self._eltStack = []
		self._inGrant = False
		self._inAccessControlList = False

		self._aclString = []
		self._aclString.append('<?xml version="1.0" encoding="UTF-8"?>')

		self._publicRead = """<Grant><Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="Group"><URI>http://acs.amazonaws.com/groups/global/AllUsers</URI></Grantee><Permission>READ</Permission></Grant>"""


	def _buildStart(self, name, attrs):
		s = '<%s' % name
		for n in attrs.keys():
			s += ' %s="%s"' % (n, attrs[n])
		s += '>'

		return s

	def parse(self, data):
		self._p.Parse(data)
		return self._grantList

	def addPublic(self):
		sRet = ''
		publicFlag = False

		#Print the first half of the acl
		for i in range(0, len(self._aclString)):
			s = self._aclString[i]
			sRet += s
			if (s == '<AccessControlList>'):
				break

		#Print Grants
		for g in self._grantList:
			sRet += g.xmlString
			if g.publicRead:
				publicFlag = True
		
		# If we don't have public read already, add it now
		if (not publicFlag):
			sRet += self._publicRead


		# Print the rest of the original acl
		for i in range(i+1, len(self._aclString)):
			sRet += self._aclString[i]

		return sRet


	def removePublic(self):
		sRet = ''

		#Print the first half of the acl
		for i in range(0, len(self._aclString)):
			s = self._aclString[i]
			sRet += s
			if (s == '<AccessControlList>'):
				break

		#Print Grants
		for g in self._grantList:
			if (not g.publicRead):
				sRet += g.xmlString
		

		# Print the rest of the original acl
		for i in range(i+1, len(self._aclString)):
			sRet += self._aclString[i]

		return sRet


	def _start_element(self, name, attrs):
		self._eltStack.append(name)

		tagString = self._buildStart(name, attrs)
		if (not self._inAccessControlList):
			self._aclString.append(tagString)
		if (name == 'Grant'):
			self._inGrant = True
			self._grantList.append(grant())
		if (name == 'AccessControlList'):
			self._inAccessControlList = True
		if (self._inGrant):
			self._grantList[-1].xmlString += tagString


	def _end_element(self, name):
		curElt = self._eltStack.pop()
		assert(curElt == name)

		if (self._inGrant):
			self._grantList[-1].xmlString += '</%s>' % name
		if (curElt == 'AccessControlList'):
			self._inAccessControlList = False
		if (curElt == 'Grant'):
			self._inGrant = False
			if self._grantList[-1].URI == 'http://acs.amazonaws.com/groups/global/AllUsers':
				if self._grantList[-1].permission in ['READ', 'FULL_CONTROL']:
					self._grantList[-1].publicRead = True

		if (not self._inAccessControlList):
			self._aclString.append('</%s>' % name)

	def _char_data(self, data):
		if (not self._inAccessControlList):
			self._aclString.append(data)
		if (not self._inGrant):
			return
		if (self._eltStack[-1] == 'DisplayName'):
			self._grantList[-1].displayName = data
		if (self._eltStack[-1] == 'Permission'):
			self._grantList[-1].permission = data
		if (self._eltStack[-1] == 'URI'):
			self._grantList[-1].URI = data

		self._grantList[-1].xmlString += data

