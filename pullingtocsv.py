import json
import requests
import time
import csv
from settings import API, DATABASE

from requests.exceptions import ConnectionError, SSLError

class PullData(object):
    def __init__(self):
        self.headers = {'Client-Id': API['CLIENTID']}
        self.offset = 0
        self.batch = 1
        self.total_values = 0

    def run(self):
	data = []
	print "I'll begin the scraping."
	stoptime = time.time() + 8640000
	while time.time() < stoptime:
		with open(time.strftime('%b%d %H.%M.%S.csv'), 'wb') as data_file:
			loopstart = time.time()
			loopstop = time.time() + 180
			csvwriter = csv.writer(data_file, delimiter=',', quotechar=' ', quoting=csv.QUOTE_MINIMAL)
			while self.batch < 100:
				print self.batch
				twitch_api_url = 'https://api.twitch.tv/kraken/streams/?stream_type=live&limit=100&offset='+str(self.getOffset())+'&language=en'
				try:
					data = (json.loads(requests.get(twitch_api_url, headers=self.getHeaders()).text)['streams']) 
				except (ValueError, ConnectionError,KeyError ,SSLError):
					time.sleep(5)
				except KeyboardInterrupt:
					print "Closing scraper gracefully!"
				
				for stream in data:
					print ('name: ' + unicode(stream['channel']['name']) + '; viewers: ' + unicode(stream['viewers']) + '; views: ' + unicode(stream['channel']['views']) + '; followers: ' + unicode(stream['channel']['followers']))
					if 0 < stream['viewers']:
						csvwriter.writerow([unicode(stream['channel']['name']).encode('utf8'), unicode(stream['viewers']).encode('utf8') + unicode(stream['channel']['views']) + unicode(stream['channel']['followers'])])

				#print "Current page Value: ", self.getOffset()
				self.nextOffset()
				self.nextBatch()
				
				# if not data:
					# print "Finished Batch!"
					# self.resetOffset()
					# self.nextBatch()
					
			while time.time() < loopstop:
				time.sleep(1)
			self.batch=0
			self.offset = 0
    def nextBatch(self):
        self.batch += 1

    def nextOffset(self):
        self.offset += 50
    
    def getOffset(self):
        return self.offset
    
    def getTotalViews(self):
        return self.total_values
    
    def getHeaders(self):
        return self.headers

    def resetOffset(self):
        self.offset = 0
    
    def increaseTotalViews(self):
        self.total_values += 1

if __name__ == '__main__':
	scraper = PullData()
	scraper.run()