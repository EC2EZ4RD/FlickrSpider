import csv
import flickrapi
import sys
import threading
import time


reload(sys)
sys.setdefaultencoding('utf8')

api_key = u'273d64a6a1a77673db1a461c2cb62bf5'
api_secret = u'4aeb5b73875458c9'
flickr=flickrapi.FlickrAPI(api_key,api_secret,cache=True)

def getNsidByCSV(csvFile):
	with open(csvFile,'rb') as csvfile:
		reader = csv.reader(csvfile)
		return list(set([row[5] for row in reader if row[0].isdigit()]))

def getUnfinishedNsid(logFile,wholeList):
	with open(logFile, 'rb') as log:
		logReader = csv.reader(log)
		logPhotoId = [row for row in logReader]
	return [item for item in wholeList if item not in logPhotoId]

def getuserId(userInfo):
	try:
		userId = userInfo.find('person').attrib['id']
	except Exception as e:
		userId = ''
	return userId

def getusername(userInfo):
	try:
		username = userInfo.find('person').find('username').text
	except Exception as e:
		username = ''
	return username

def getrealname(userInfo):
	try:
		realname = userInfo.find('person').find('realname').text
	except Exception as e:
		realname = ''
	return realname

def getlocation(userInfo):
	try:
		location = userInfo.find('person').find('location').text
	except Exception as e:
		location = ''
	return location

def getphotosurl(userInfo):
	try:
		photosurl = userInfo.find('person').find('photosurl').text
	except Exception as e:
		photosurl = ''
	return photosurl

def getprofileurl(userInfo):
	try:
		profileurl = userInfo.find('person').find('profileurl').text
	except Exception as e:
		profileurl = ''
	return profileurl

def getmobileurl(userInfo):
	try:
		mobileurl = userInfo.find('person').find('mobileurl').text
	except Exception as e:
		mobileurl = ''
	return mobileurl

def getfristdatetaken(userInfo):
	try:
		frstdatetaken = userInfo.find('person').find('photos').find('firstdatetaken').text
	except Exception as e:
		frstdatetaken = ''
	return frstdatetaken

def getcount(userInfo):
	try:
		count = userInfo.find('person').find('photos').find('count').text
	except Exception as e:
		count = ''
	return count

def getUserInfo(userInfo):
	return (getuserId(userInfo),getusername(userInfo),getrealname(userInfo),getlocation(userInfo),getphotosurl(userInfo),getprofileurl(userInfo),getmobileurl(userInfo),getfristdatetaken(userInfo),getcount(userInfo))

def getCSVPhotoInfo(listNsid,resultFile,logFile):
	result = file(resultFile, 'ab')
	writer = csv.writer(result)
	writer.writerow(['userId','username','realname','location','photosurl','profileurl','mobileurl','fristdatetaken','count'])
	result.close()
	for nsid in listNsid:
		userInfo = flickr.people.getInfo(user_id = nsid)
		infoOfUser = getUserInfo(userInfo)
		print 'userId', infoOfUser[0]
		with open(resultFile,'ab') as result:
			userInfoWriter = csv.writer(result)
			userInfoWriter.writerow(infoOfUser)
		with open(logFile,'ab') as log:
			logWriter = csv.writer(log)
			logWriter.writerow([nsid])
			
def multiThread(threads,downList,logFile,numThread):
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
		threads.append(threading.Thread(target=getCSVPhotoInfo,args=(downList[i*lenSlice:min((i+1)*lenSlice,lendownList)],'UserInfo'+str(i)+'_'+curTime+'.csv',logFile,)))

if __name__ == '__main__':
	curTime = str(int(time.time()))
	DataOfGuangzhou = 'DataOfGuangzhou_numerical_part.csv'
	logFile = 'logNsid.csv'
	listNsid = getUnfinishedNsid(logFile,getNsidByCSV(DataOfGuangzhou))
	threads = []
	multiThread(threads,listNsid,logFile,20)
	for t in threads:
		t.setDaemon(True)
		t.start()
	for t in threads:
		t.join()

