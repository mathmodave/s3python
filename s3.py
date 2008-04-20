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

def usage(onError):
	if (onError):
		print "Error - command not recognised"
	else:
		print """s3python version 0.1, Copyright (C) 2008 David Oxley
s3python comes with ABSOLUTELY NO WARRANTY; for details see v2 of the GPL
This is free software, and you are welcome to redistribute it
under certain conditions; see v2 of the GPL for details."""
	print """Usage: s3 [OPTION...] 

Examples:
  s3 -L                                # List all buckets and their contents
  s3 -l mybucket                       # List the contents of 'mybucket'
  s3 -b mybucket -p /foo/bar -k bar    # Upload '/foo/bar' to the key 'bar' in 'mybucket'
  s3 -b mybucket -g bar -o /foo/bar    # Download the object with key 'bar' from 'mybucket', save as /foo/bar
  s3 -b mybucket -g bar                # Download the object with key 'bar' from 'mybucket', output to stdout
  s3 -b mybucket -X bar                # Delete the object with key 'bar' from 'mybucket'
  s3 -b mybucket -C                    # Create mybucket
  s3 -b mybucket -D                    # Delete mybucket
  s3 -b mybucket -k bar -a publicread  # Make the object 'bar' in 'mybucket' publicly readable
  s3 -b mybucket -k bar -a private     # Remove the public read permission from 'bar' in 'mybucket'

In cases of success, s3python is either silent or will output the HTTP header code to stdout.
When s3python encounters errors, the full headers and output is sent to stdout.

Two files: publickey and privatekey are expected to be found in the directory containing s3commands.py
Each of these files should contain the AmazonS3 issued "public" and "private" keys

Report bugs to s3-code@psi.epsilon.org.uk"""
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
				usage(True)
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
		raw_opts, args = getopt.getopt(sys.argv[1:], 'hCDLl:p:g:X:o:b:a:k:')
	except:
		usage(True)

	processOpts(raw_opts)

	if opts_dict.has_key('h'): # Help
		usage(False)
		sys.exit(0)

 	if opts_dict.has_key('L'): #List all
		listAllBuckets()
		sys.exit(0)
	elif opts_dict.has_key('l'): #List bucket
		listBucket(opts('l'))
		sys.exit(0)

	if opts_dict.has_key('D'): # Remove Bucket
		if (opts('b') == ''):
			usage(True)

		s3commands.doDeleteBucket(opts('b'))
		sys.exit(0)

	if opts_dict.has_key('C'): # Create Bucket
		if (opts('b') == ''):
			usage(True)

		s3commands.doPutBucket(opts('b'))
		sys.exit(0)

	if opts_dict.has_key('p'): #Put (Upload)
		if (opts('b') == '') | (opts('p') == '') | (opts('k') == ''):
			usage(True)

		s3commands.doPutObjectFromFile(opts('b'), opts('p'), opts('k'))
		sys.exit(0)

	if opts_dict.has_key('g'): #Get (Download)
		if (opts('b') == '') | (opts('g') == ''):
			usage(True)
	
		req, header, data = s3commands.doGetObject(opts('b'), opts('g'), opts('o'))

		if (opts('o') == ''):
			print data
		sys.exit(0)

	if opts_dict.has_key('X'): #Delete
		if (opts('b') == ''):
			usage(True)

		req, header, data = s3commands.doDeleteObject(opts('b'), opts('X'))
	
		sys.exit(0)

	if opts_dict.has_key('a'): # ACL
		if (opts('a') == '') | (opts('b') == '') | (opts('k') == ''):
			usage(True)

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

