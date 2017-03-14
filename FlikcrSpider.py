#coding=utf-8
import flickrapi
import csv
import sys
import threading

reload(sys)
sys.setdefaultencoding('utf8')

api_key = u'273d64a6a1a77673db1a461c2cb62bf5'
api_secret = u'4aeb5b73875458c9'
flickr=flickrapi.FlickrAPI(api_key,api_secret,cache=True)


def getNSID(photoInfo):
	try:
		nsid = photoInfo.find('photo').find('owner').attrib['nsid']
	except Exception as e:
		nsid = ''
	return nsid

def getUsername(photoInfo):
	try:
		username = photoInfo.find('photo').find('owner').attrib['username']
	except Exception as e:
		username = ''
	return username

def getTitle(photoInfo):
	try:
		title = photoInfo.find('photo').find('title').text
	except Exception as e:
		title = ''
	return title

def getViews(photoInfo):
	try:
		views = photoInfo.find('photo').attrib['views']
	except Exception as e:
		views = ''
	return views

def getLongtitude(photoInfo):
	try:	 
		longitude = photoInfo.find('photo').find('location').attrib['longitude']
	except Exception as e:
		longitude = ''
	return longitude

def getLatitude(photoInfo):
	try:
		latitude = photoInfo.find('photo').find('location').attrib['latitude']
	except Exception as e:
		latitude = ''
	return latitude

def getTag(photoInfo):
	tag = ''
	try:
		tags = [tag.text for tag in photoInfo.find('photo').find('tags').findall('tag')]
	except Exception as e:
		pass
	finally:
		if tags != []:
			tag = reduce(lambda x, y: x + ',' + y, tags)
	return tag

def getComments(commentsInfo):
	comment = ''
	try:
		comments = [comment.text for comment in commentsInfo.find('comments').findall('comment')]
	except Exception as e:
		pass
	finally:
		if comments != []:
			comment = reduce(lambda x, y: x + ',' + y, comments)
	return comment


def getCSVPhotoInfo(photoId):
	photoInfo = flickr.photos.getInfo(photo_id = photoId)
	commentsInfo = flickr.photos.comments.getList(photo_id = photoId)
	tempInfo = (getNSID(photoInfo),getUsername(photoInfo),getTitle(photoInfo),getViews(photoInfo),getLongtitude(photoInfo),getLatitude(photoInfo),getTag(photoInfo),getComments(commentsInfo))
	return tempInfo

def csvInfo(listBbox,resultFile,logFile,numThread):
	result = file(resultFile, 'ab')
	writer = csv.writer(result)
	writer.writerow(['photoId', 'url', 'date','nsid','username','title','views','longtitude','latitude','tags','comments'])
	for box in listBbox:
		photos=flickr.walk(bbox = box[0], extras = 'url_c,date_taken')
		print numThread,photos
		photoInfo = ()
		count = 0
		for photo in photos:
			url = photo.get('url_c')
			photoId = photo.get('id')
			datetaken = photo.get('datetaken')
			tempInfo = getCSVPhotoInfo(photoId)
			#we can use json to ecapsulate data
			photoInfo = (photoId,url,datetaken)+tempInfo
			writer.writerow(photoInfo)
			count += 1
			print '#'+ str(numThread) + '#' + str(count)
		with open(logFile,'ab') as log:
			logWriter = csv.writer(log)
			logWriter.writerow([box[1]])
	result.close()

def readNeedIdx(csvIdxFile):
	with open(csvIdxFile,'rb') as csvfile:
		reader = csv.reader(csvfile)
		return [row[0] for row in reader]

def readLocationByIdx(needIdx,csvBboxFile,logFile):
	#input : list of string idx
	#output : list bbox in which each item contains location
	listBbox = []
	with open(logFile, 'rb') as log:
		logReader = csv.reader(log)
		logIdx = [row[0] for row in logReader]
	with open(csvBboxFile,'rb') as csvfile:
		reader = csv.reader(csvfile)
		for row in reader:
			if row[4] in needIdx and row[4] not in logIdx:
				listBbox.append([reduce(lambda x, y : x + ',' + y, [row[0],row[1],row[2],row[3]]),row[4]])
	return listBbox

def multiThread(threads,listBbox,logFile,numThread):
	lenListBbox = len(listBbox)
	if lenListBbox > numThread:
		lenSlice = lenListBbox/numThread
	for i in range(numThread):
		threads.append(threading.Thread(target=csvInfo,args=(listBbox[i*lenSlice:min((i+1)*lenSlice,lenListBbox)],'result'+str(i)+'.csv',logFile,i,)))

if __name__ == '__main__':
	#box = "112.0340550000, 22.6138310000, 114.6970640000, 23.9268780000"
	#csvInfo(box)
	csvBboxFile = 'bbox_allv2.csv'
	logFile = 'log.csv'
	csvIdxFile = 'selected_bbox_idx_part01.csv'
	needIdx = readNeedIdx(csvIdxFile)
	listBbox = readLocationByIdx(needIdx,csvBboxFile,logFile)
	threads = []
	multiThread(threads,listBbox,logFile,10)
	for t in threads:
		t.setDaemon(True)
		t.start()
	t.join()


#print getNSID(2946221901)
#print getCSVPhotoInfo(32580997843)

#photoInfo = flickr.photos.getInfo(photo_id = 2946221901)
#commentsInfo = flickr.photos.comments.getList(photo_id = 32580997843)
#nsid = photoInfo.find('photo').find('owner').attrib['nsid']
#print nsid
#print reduce(lambda x, y: x + ',' + y, [comments.text for comments in commentsInfo.find('comments').findall('comment')])