#!/usr/bin/python

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

import sys
import getopt
import s3commands
import s3parser

def usage():
	print 'RTFM!'
	sys.exit(-1)

opts_dict = {}
exclusive_opts = ['l', 'L', 'p', 'g', 'X', 'a', 'C', 'D']

def opts(s):
	if opts_dict.has_key(s):
		return opts_dict[s]
	else:
		return ''

def processOpts(raw_opts):
	
	for o, a in raw_opts:
		opts_dict[o[1:]] = a

	exclusiveFlag = False;

	for x in opts_dict.keys():
		if (x in exclusive_opts):
			if exclusiveFlag:
				usage()
			else:
				exclusiveFlag = True


def listAllBuckets():
	bucketList = s3commands.doListAllBuckets()
	for bucket in bucketList:
		print bucket.name
		keyList = s3commands.doGetBucket(bucket.name)
		for key in keyList:
			print '',
			key.dump()

def listBucket(theBucket):
	keyList = s3commands.doGetBucket(theBucket)
	for key in keyList:
		print '',
		key.dump()


if __name__ == '__main__':
	try:
		raw_opts, args = getopt.getopt(sys.argv[1:], 'CDLl:p:g:X:o:b:a:k:')
	except:
		usage()

	processOpts(raw_opts)

 	if opts_dict.has_key('L'): #List all
		listAllBuckets()
		sys.exit(0)
	elif opts_dict.has_key('l'): #List bucket
		listBucket(opts('l'))
		sys.exit(0)

	if opts_dict.has_key('D'): # Remove Bucket
		if (opts('b') == ''):
			usage()

		s3commands.doDeleteBucket(opts('b'))
		sys.exit(0)

	if opts_dict.has_key('C'): # Create Bucket
		if (opts('b') == ''):
			usage()

		s3commands.doPutBucket(opts('b'))
		sys.exit(0)

	if opts_dict.has_key('p'): #Put (Upload)
		if (opts('b') == '') | (opts('p') == '') | (opts('k') == ''):
			usage()

		s3commands.doPutObjectFromFile(opts('b'), opts('p'), opts('k'))
		sys.exit(0)

	if opts_dict.has_key('g'): #Get (Download)
		if (opts('b') == '') | (opts('g') == ''):
			usage()
	
		req, header, data = s3commands.doGetObject(opts('b'), opts('g'), opts('o'))

		if (opts('o') == ''):
			print data
		sys.exit(0)

	if opts_dict.has_key('X'): #Delete
		if (opts('b') == ''):
			usage()

		req, header, data = s3commands.doDeleteObject(opts('b'), opts('X'))
	
		sys.exit(0)

	if opts_dict.has_key('a'): # ACL
		if (opts('a') == '') | (opts('b') == '') | (opts('k') == ''):
			usage()

		targetKey = '%s?acl' % opts('k')

		req, header, data = s3commands.doGetObject(opts('b'), targetKey, '')
		if (header[0].find('HTTP/1.1 200 OK') < 0):
			sys.exit(0)
			
		theParser = s3parser.aclParser()
		theParser.parse(data)

		if (opts('a') == 'publicread'):
			newACL = theParser.addPublic()
		else:
			newACL = theParser.removePublic()

		open('/tmp/acl', 'w').write(newACL)
		req, header, data = s3commands.doPutObjectFromFile(opts('b'), '/tmp/acl', targetKey)
		print header[0][:-2]

