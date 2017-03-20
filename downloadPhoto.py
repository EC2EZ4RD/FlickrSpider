import flickrapi
import csv
#import time
import urllib2
import imghdr
import threading
import os

api_key = u'273d64a6a1a77673db1a461c2cb62bf5'
api_secret = u'4aeb5b73875458c9'

flickr=flickrapi.FlickrAPI(api_key,api_secret,cache=True)


def readPhotoIdUrl(csvPhotoUrl):
	with open(csvPhotoUrl,'rb') as csvfile:
		reader = csv.reader(csvfile)
		return [(row[0],row[1]) for row in reader if row[0].isdigit()]

def unfinishedPhotoUrl(logFile,wholeList):
	with open(logFile, 'rb') as log:
		logReader = csv.reader(log)
		logPhotoId = [row[0] for row in logReader]
	return [item for item in wholeList if item[0] not in logPhotoId]

def downPhoto(downList):
	for item in downList:
		#print item
		#start = time.clock()
		sizeInfo = flickr.photos.getSizes(photo_id = item[0])
		if item[1] == 'NULL':
			urlPhoto = [size.attrib['source'] for size in sizeInfo.find('sizes').findall('size')][-1]
		else:
			urlPhoto = item[1]
		print 'urlPhoto', urlPhoto
		content = urllib2.urlopen(urlPhoto).read()
		imgtype = imghdr.what('', h=content)
		if not imgtype:
			imgtype = 'txt'
		with open(os.path.dirname(os.path.realpath(__file__))+'\\photo\\{}.{}'.format(item[0], imgtype), 'wb') as f:
			f.write(content)
		with open('logPhotoId.csv','ab') as log:
			logWriter = csv.writer(log)
			logWriter.writerow((item[0],urlPhoto))
		#end = time.clock()
		#print "read: %f s" % (end - start)

def multiThread(threads,downList,numThread):
	lendownList = len(downList)
	if lendownList >= numThread:
		lenSlice = lendownList/numThread
		realNumThread = numThread
	else:
		realNumThread = lendownList
		lenSlice = 1
		print 'lenSlice', lenSlice
	print 'numThread', realNumThread+1
	for i in range(realNumThread+1):
		threads.append(threading.Thread(target=downPhoto,args=(downList[i*lenSlice:min((i+1)*lenSlice,lendownList)],)))

if __name__ == '__main__':
	csvPhotoUrl = 'DataOfGuangzhou_url_part.csv'
	finishedPhotoList = 'logPhotoId.csv'
	downList = unfinishedPhotoUrl(finishedPhotoList, readPhotoIdUrl(csvPhotoUrl))
	threads = []
	multiThread(threads,downList,20)
	for t in threads:
		t.setDaemon(True)
		t.start()
	for t in threads:
		t.join()
	#print downList