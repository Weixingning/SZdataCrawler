# coding=UTF8

from urllib.request import urlopen
from bs4 import BeautifulSoup
import requests
import re
import time
import json


def gethtml(url):

	r = requests.get(url)
	r.raise_for_status()
	r.encoding = 'utf8'
	return r.text


def getDataListByCity8090Url(outputFile, url):

	tempFile = "data_temp.txt"

	with open(tempFile, 'w+', encoding='utf8') as foutput:

		html = gethtml(url)
		foutput.write(F'{html}\n')
		print('city8090采集完成')

	with open(tempFile, "r", encoding='utf8') as finput, open(outputFile, "w+", encoding='utf8') as foutput:

		lines = finput.read()

		lineData = re.findall('\"\_blank\"\>(.*?)\<', lines)
		lineData = set(lineData)
		for i in lineData:
			foutput.write(F'{i}\n')
		print('总共发现：' + str(len(lineData)))


def getDataListByBaiduUrl(outputFile, keyword, pageMax):

	tempFile = "data_temp.txt"

	with open(tempFile, 'w+', encoding='utf8') as foutput:

		for i in range(1, pageMax):
			url = F'http://api.map.baidu.com/?qt=s&c=131&wd=深圳%20{keyword}&rn=50&pn={i}&ie=utf-8&res=api&ak=E4805d16520de693a3fe707cdc962045'
			print(url)
			html = gethtml(url)
			foutput.write(F'{html}\n')
		print('百度原始数据采集完成')

	with open(tempFile, "r", encoding='utf8') as finput, open(outputFile, "w+", encoding='utf8') as foutput:

		lines = finput.read()

		lineData = re.findall('\},\"name\"\:\"(.*?)\".*?poi_address\"\:\"(.*?)\"', lines)
		# 针对招待所等，因为网页中会掺入携程去哪儿艺龙等含name字段的订房链接信息，要先在扒下来的文件中手动过滤一遍，因此要用以下特殊这行
		# lineData = re.findall('\"name\"\:\"(.*?)\".*?addr\"\:\"(.*?)\"', lines)
		for i in lineData:
			foutput.write(F'{i[0]},{i[1]}\n')
			# foutput.write(F'{i}\n')
		print('总共发现：' + str(len(lineData)))


# 通过上一步获取的地址，拿到经纬度等大致信息
def getRawDataByCity8090Url(inputFile, outputFile, primaryTitle, secondaryTitle):

	with open(inputFile, "r", encoding='utf8') as finput, open(outputFile, "w+", encoding='utf8') as foutput:

		lines = finput.readlines()
		count = 0

		for i in range(len(lines)):

			if i % 1000 == 0 and i != 0:
				time.sleep(10)

			lines[i] = lines[i].strip('\n')

			area_name = lines[i]

			Ak = 'rxNZN3FlDIC4RsSDLwGQB9chqxWBtc3E'
			SK = 'L8AalX71UNCTGa2i5SHDGgpmrvxI9FwS'

			urlHead = 'http://api.map.baidu.com'
			wholeStr = urlHead + '/place/v2/suggestion?query=%s&region=深圳&city_limit=true&output=json&ak=%s' % (area_name, Ak)

			request = requests.get(wholeStr)
			if request.status_code == 200:
				# print(request.text)
				lat = re.findall(r'\"lat\":(.+?),', request.text)
				lng = re.findall(r'\"lng\":(.+?)\n', request.text)
				uid = re.findall(r'\"uid\":\"(.+?)\",', request.text)
				province = re.findall(r'\"province\":\"(.+?)\",', request.text)
				city = re.findall(r'\"city\":\"(.+?)\",', request.text)
				district = re.findall(r'\"district\":\"(.+?)\",', request.text)
				business = re.findall(r'\"business\":\"(.+?)\",', request.text)
				cityid = re.findall(r'\"cityid\":\"(.+?)\"\n', request.text)

				lng = fixContent(lng, None)
				lat = fixContent(lat, None)
				uid = fixContent(uid, None)
				province = fixContent(province, None)
				city = fixContent(city, None)
				district = fixContent(district, None)
				business = fixContent(business, None)
				cityid = fixContent(cityid, None)

				if lng is not None and lat is not None:
					result = primaryTitle + ',' + secondaryTitle + ',' + area_name + ',' + lng + ',' + lat +\
							',' + uid + ',' + province + ',' + city + ',' + district + ',' + business + ',' + cityid
					print('总共条数：' + str(len(lines)) + '，当前第' + str(i) + '条——' + result)
					if 'null' not in lng and 'null' not in lat:
						count = count + 1
			else:
				result = 'error'
				print('总共条数：' + str(len(lines)) + '，当前第' + str(i) + '条——' + result)

			foutput.write(F'{result}\n')
		print('总共条数：' + str(len(lines)))
		print('有效条数：' + str(count))


def getRawDataByBaiduUrl(inputFile, outputFile, primaryTitle, secondaryTitle):

	with open(inputFile, "r", encoding='utf8') as finput, open(outputFile, "w+", encoding='utf8') as foutput:

		lines = finput.readlines()
		count = 0

		# 百度采集
		for i in range(len(lines)):

			if i % 1000 == 0 and i != 0:
				time.sleep(10)

			lines[i] = lines[i].strip('\n')
			linesPoint = lines[i].split(',')
			# addr = re.findall(r',(\S+)', str(lines[i]))
			# addr = addr[0]
			area_name = linesPoint[0]
			address = linesPoint[1]

			Ak = 'rxNZN3FlDIC4RsSDLwGQB9chqxWBtc3E'
			SK = 'L8AalX71UNCTGa2i5SHDGgpmrvxI9FwS'

			urlHead = 'http://api.map.baidu.com'
			# # params = {}
			# # params['address'] = '黄阁中路黄阁翠苑1层102号'
			# # params['output'] = 'json'
			# # params['ak'] = Ak + SK
			# # # params['city'] = '深圳'
			#
			# queryStr = '/geocoder/v2/?'
			# tempStr = queryStr + urllib.parse.urlencode(params)

			# wholeStr = urlHead + '/geocoder/v2/?address=%s&output=json&ak=%s' % (addr, Ak)
			# request = requests.get(wholeStr)
			#
			# code = request.status_code
			# if code == 200:
			# 	lng = re.findall(r"\"lng\":(.+?),\"", request.text)
			# 	lat = re.findall(r"\"lat\":(.+?)},\"", request.text)
			# 	# level = re.findall(r"\"level\":\"(.+?)\"}", request.text)
			# 	if lng is not None and lat is not None:
			# 	lat = re.findall(r'\"lat\":(.+?),', request.text)
			# 	lng = re.findall(r'\"lng\":(.+?)\n', request.text)
			# 	uid = re.findall(r'\"uid\":\"(.+?)\",', request.text)
			# 	province = re.findall(r'\"province\":\"(.+?)\",', request.text)
			# 	city = re.findall(r'\"city\":\"(.+?)\",', request.text)
			# 	district = re.findall(r'\"district\":\"(.+?)\",', request.text)
			# 	business = re.findall(r'\"business\":\"(.+?)\",', request.text)
			# 	cityid = re.findall(r'\"cityid\":\"(.+?)\"\n', request.text)
			# 		result = lines[i] + ',' + lng + ',' + lat
			# 		print(result)
			# else:
			# 	result = lines[i] + ',' + 'error'
			# 	print('error')

			urlHead = 'http://api.map.baidu.com'
			wholeStr = urlHead + '/place/v2/suggestion?query=%s&region=深圳&city_limit=true&output=json&ak=%s' % (area_name, Ak)

			request = requests.get(wholeStr)
			if request.status_code == 200:
				# print(request.text)
				lat = re.findall(r'\"lat\":(.+?),', request.text)
				lng = re.findall(r'\"lng\":(.+?)\n', request.text)
				uid = re.findall(r'\"uid\":\"(.+?)\",', request.text)
				province = re.findall(r'\"province\":\"(.+?)\",', request.text)
				city = re.findall(r'\"city\":\"(.+?)\",', request.text)
				district = re.findall(r'\"district\":\"(.+?)\",', request.text)
				business = re.findall(r'\"business\":\"(.+?)\",', request.text)
				cityid = re.findall(r'\"cityid\":\"(.+?)\"\n', request.text)

				lng = fixContent(lng, None)
				lat = fixContent(lat, None)
				uid = fixContent(uid, None)
				province = fixContent(province, None)
				city = fixContent(city, None)
				district = fixContent(district, None)
				address = fixContent(address, None)
				business = fixContent(business, None)
				cityid = fixContent(cityid, None)

				if lng is not None and lat is not None:
					result = primaryTitle + ',' + secondaryTitle + ',' + area_name + ',' + lng + ',' + lat +\
							',' + uid + ',' + province + ',' + city + ',' + district + ',' + address +\
							 ',' + business + ',' + cityid
					print('总共条数：' + str(len(lines)) + '，当前第' + str(i) + '条——' + result)
					if 'null' not in lng and 'null' not in lat:
						count = count + 1
			else:
				result = 'error'
				print('总共条数：' + str(len(lines)) + '，当前第' + str(i) + '条——' + result)

			foutput.write(F'{result}\n')

		print('总共条数：' + str(len(lines)))
		print('有效条数：' + str(count))


# 获取详细信息
def getDetailData(sourceWeb, inputFile, outputFile):

	with open(inputFile, "r", encoding='utf8') as finput, open(outputFile, "w+", encoding='utf8') as foutput:
		lines = finput.readlines()
		count = 0

		# # ======================================== 并发模式 ================================================
		# nums = len(lines)
		# print('nums:' + str(nums))
		# pointCount = 0
		# point = ""
		# while (nums > 10):
		# 	point = point + str(pointCount*10) + ','
		# 	nums = nums - 10
		# 	pointCount = pointCount + 1
		# point = point + str(nums)
		# point = point.split(',')
		#
		# # for i in range(len(point)):
		# for i in range(1):
		# 	if i != len(point) - 1:
		#
		# 		for j in range(10):
		# 			print(int(point[i]) + j)
		#
		# 		function_type = [0]*10
		# 		area_type = [0]*10
		# 		area_name = [0]*10
		# 		lng = [0]*10
		# 		lat = [0]*10
		# 		uids = ''
		# 		province = [0]*10
		# 		city = [0]*10
		# 		district = [0]*10
		# 		business = [0]*10
		# 		cityid = [0]*10
		#
		# 		street_id = [0]*10
		# 		telephone = [0]*10
		# 		detail = [0]*10
		#
		# 		for j in range(10):
		# 			linesPoint = lines[int(point[i]) + j].strip('\n')
		# 			linesPoint = linesPoint.split(',')
		#
		# 			function_type[j] = linesPoint[0]
		# 			area_type[j] = linesPoint[1]
		# 			area_name[j] = linesPoint[2]
		# 			lng[j] = linesPoint[3]
		# 			lat[j] = linesPoint[4]
		# 			# uid[j] = linesPoint[5]
		# 			province[j] = linesPoint[6]
		# 			city[j] = linesPoint[7]
		# 			district[j] = linesPoint[8]
		# 			business[j] = linesPoint[9]
		# 			cityid[j] = linesPoint[10]
		#
		# 			if j != 10 - 1:
		# 				uids = uids + linesPoint[5] + ','
		# 			else:
		# 				uids = uids + linesPoint[5]
		# 		print(uids)
		# 		print()
		#
		# 		Ak = 'rxNZN3FlDIC4RsSDLwGQB9chqxWBtc3E'
		# 		SK = 'L8AalX71UNCTGa2i5SHDGgpmrvxI9FwS'
		#
		# 		urlHead = 'http://api.map.baidu.com'
		# 		wholeStr = urlHead + '/place/v2/detail?uids=%s&output=json&scope=2&ak=%s' % (uids, Ak)
		#
		# 		request = requests.get(wholeStr)
		#
		# 		if request.status_code == 200:
		# 			# print(request.text)
		#
		# 			# 将返回的json转化成字典，提取'result'部分之后，在转换成str，并切分
		# 			jsonPart = json.loads(request.text)
		# 			jsonPartResult = jsonPart['result']
		# 			jsonPartResult = str(jsonPartResult)
		# 			jsonPartResult = jsonPartResult.split("}, {'uid':")
		#
		# 		# 选取对应的时候要注意，跳过null的那一条
		# 		jsonPartResultCount = 0
		# 		for j in range(10):
		# 			if 'null' not in lng[j] and 'null' not in lat[j]:
		#
		# 				print(jsonPartResult[jsonPartResultCount])
		#
		# 				address = re.findall(r"'address': '(.+?)',", jsonPartResult[jsonPartResultCount])
		# 				street_id = re.findall(r"'street_id': '(.+?)',", jsonPartResult[jsonPartResultCount])
		# 				telephone = re.findall(r"'telephone': '(.+?)',", jsonPartResult[jsonPartResultCount])
		# 				type = re.findall(r"'type': '(.+?)',", jsonPartResult[jsonPartResultCount])
		# 				overall_rating = re.findall(r"'overall_rating': '(.+?)',", jsonPartResult[jsonPartResultCount])
		# 				service_rating = re.findall(r"'service_rating': '(.+?)',", jsonPartResult[jsonPartResultCount])
		# 				environment_rating = re.findall(r"'environment_rating': '(.+?)',", jsonPartResult[jsonPartResultCount])
		# 				detail = re.findall(r"'detail': (.+?)", jsonPartResult[jsonPartResultCount])
		#
		# 				print(address)
		# 				print(street_id)
		# 				print(telephone)
		# 				print(type)
		# 				print(overall_rating)
		# 				print(service_rating)
		# 				print(environment_rating)
		# 				print(detail)
		#
		# 				jsonPartResultCount = jsonPartResultCount + 1
		# 			else:
		# 				print("该条无有效json")
		# 	else:
		# 		# 这部分是剩下的，余下的
		#
		# 		for j in range(int(point[i])):
		# 			print(int(point[i - 1]) + 10 + j)



		# ======================================== 非并发模式 ================================================
		# for i in range(len(lines)):
		for i in range(len(lines)):

			if i % 100 == 0 and i != 0:
				time.sleep(10)

			linesPoint = lines[i].strip('\n')
			linesPoint = linesPoint.split(',')

			if "City8090" in sourceWeb:
				# 检索数据来源是City8090
				# result = primaryTitle + ',' + secondaryTitle + ',' + area_name + ',' + lng + ',' + lat + \
				# 		 ',' + uid + ',' + province + ',' + city + ',' + district + ',' + business + ',' + cityid
				function_type = linesPoint[0]
				area_type = linesPoint[1]
				area_name = linesPoint[2]
				lng = linesPoint[3]
				lat = linesPoint[4]
				uid = linesPoint[5]
				province = linesPoint[6]
				city = linesPoint[7]
				district = linesPoint[8]
				business = linesPoint[9]
				cityid = linesPoint[10]

			# if "Baidu" in sourceWeb:
				# # 检索数据来源是Baidu
				# # result = primaryTitle + ',' + secondaryTitle + ',' + area_name + ',' + lng + ',' + lat +\
				# # 		',' + uid + ',' + province + ',' + city + ',' + district + ',' + address +\
				# # 		 ',' + business + ',' + cityid
				# function_type = linesPoint[0]
				# area_type = linesPoint[1]
				# area_name = linesPoint[2]
				# lng = linesPoint[3]
				# lat = linesPoint[4]
				# uid = linesPoint[5]
				# province = linesPoint[6]
				# city = linesPoint[7]
				# district = linesPoint[8]
				# address = linesPoint[9]
				# business = linesPoint[10]
				# cityid = linesPoint[11]

			Ak = 'rxNZN3FlDIC4RsSDLwGQB9chqxWBtc3E'
			SK = 'L8AalX71UNCTGa2i5SHDGgpmrvxI9FwS'

			urlHead = 'http://api.map.baidu.com'
			wholeStr = urlHead + '/place/v2/detail?uid=%s&output=json&scope=2&ak=%s' % (uid, Ak)

			if 'null' not in lng and 'null' not in lat:
				request = requests.get(wholeStr)

				if request.status_code == 200:
					# print(request.text)
					lngDetail = re.findall(r'\"lng\":(.+?),', request.text)
					latDetail = re.findall(r'\"lat\":(.+?)\n', request.text)
					addressDetail = re.findall(r'\"address\":\"(.+?)\"', request.text)
					street_id = re.findall(r'\"street_id\":\"(.+?)\"', request.text)
					telephone = re.findall(r'\"telephone\":\"(.+?)\"', request.text)
					type = re.findall(r"\"type\":\"(.+?)\"", request.text)
					overall_rating = re.findall(r"\"overall_rating\":\"(.+?)\"", request.text)
					service_rating = re.findall(r"\"service_rating\":\"(.+?)\"", request.text)
					environment_rating = re.findall(r"\"environment_rating\":\"(.+?)\"", request.text)
					image_num = re.findall(r"\"image_num\":\"(.+?)\"", request.text)
					comment_num = re.findall(r"\"comment_num\":\"(.+?)\"", request.text)
					shop_hours = re.findall(r"\"shop_hours\":\"(.+?)\"", request.text)
					content_tag = re.findall(r"\"content_tag\":\"(.+?)\"", request.text)
					detail = re.findall(r'\"detail\":(.+?)\n', request.text)
					detail_url = re.findall(r"\"detail_url\":\"(.+?)\"", request.text)

					lng = fixContent(lngDetail, lng)
					lat = fixContent(latDetail, lat)
					if "City8090" in sourceWeb:
						address = fixContent(addressDetail, None)
					if "Baidu" in sourceWeb:
						address = fixContent(addressDetail, address)
					street_id = fixContent(street_id, None)
					telephone = fixContent(telephone, None)
					type = fixContent(type, None)
					overall_rating = fixContent(overall_rating, None)
					service_rating = fixContent(service_rating, None)
					environment_rating = fixContent(environment_rating, None)
					image_num = fixContent(image_num, None)
					comment_num = fixContent(comment_num, None)
					shop_hours = fixContent(shop_hours, None)
					content_tag = fixContent(content_tag, None)
					detail = fixContent(detail, None)
					detail_url = fixContent(detail_url, None)

					if lng is not None and lat is not None:
						result = function_type + ',' + area_type + ',' + area_name + ',' + lng + ',' + lat +\
								 ',' + uid + ',' + address + ',' + province + ',' + city + ',' + district +\
								 ',' + business + ',' + cityid + ',' + street_id + ',' + telephone + ',' + type +\
								 ',' + overall_rating + ',' + service_rating + ',' + environment_rating +\
								 ',' + image_num + ',' + comment_num + ',' + shop_hours + ',' + content_tag +\
								 ',' + detail + ',' + detail_url
						print('总共条数：' + str(len(lines)) + '，当前第' + str(i) + '条——' + result)
						if 'null' not in lng and 'null' not in lat:
							count = count + 1
				else:
					result = 'error'
					print('总共条数：' + str(len(lines)) + '，当前第' + str(i) + '条——' + result)
			else:
				# 在之前就没有详细信息，不用去查，直接设置默认值
				address = 'null'
				street_id = 'null'
				telephone = 'null'
				type = 'null'
				overall_rating = 'null'
				service_rating = 'null'
				environment_rating = 'null'
				image_num = 'null'
				comment_num = 'null'
				shop_hours = 'null'
				content_tag = 'null'
				detail = 'null'
				detail_url = 'null'

				result = function_type + ',' + area_type + ',' + area_name + ',' + lng + ',' + lat + \
						 ',' + uid + ',' + address + ',' + province + ',' + city + ',' + district + \
						 ',' + business + ',' + cityid + ',' + street_id + ',' + telephone + ',' + type + \
						 ',' + overall_rating + ',' + service_rating + ',' + environment_rating + \
						 ',' + image_num + ',' + comment_num + ',' + shop_hours + ',' + content_tag + \
						 ',' + detail + ',' + detail_url
				print('总共条数：' + str(len(lines)) + '，当前第' + str(i) + '条——' + result)

			foutput.write(F'{result}\n')

		print('总共条数：' + str(len(lines)))
		print('有效条数：' + str(count))


# 对内容进行修正，偏向前一个数据的内容
def fixContent(word1, word2):
	if word1 is not None and len(word1) is not 0:

		if isinstance(word1, list):
			word1 = word1[0]
		else:
			word1 = word1

		if word2 is not None and len(word2) is not 0:
			if isinstance(word2, list):
				word2 = word2[0]
			else:
				word2 = word2

			# 很粗糙的作对比，以字段的长短判断，采用长的
			return(word1 if len(word1) > len(word2) else word2)
		else:
			return(word1)
	elif word2 is not None and len(word2) is not 0:
		if isinstance(word2, list):
			word2 = word2[0]
		else:
			word2 = word2
		return(word2)
	else:
		return('null')


if __name__ == '__main__':

	timestamp = str(time.strftime("%Y-%m-%d %H-%M-%S", time.localtime()))
	sourceWeb = "City8090"
	changeName = 'changkuang'
	primaryTitle = '公司企业'
	secondaryTitle = '厂矿'
	keyword = secondaryTitle

	if "Baidu" in sourceWeb:
		if secondaryTitle == "超市":
			pageMax = 16
		elif secondaryTitle == "商场":
			pageMax = 50
		elif secondaryTitle == "驾校":
			pageMax = 15
		elif secondaryTitle == "汽车配件":
			pageMax = 11
		elif secondaryTitle == "招待所":
			pageMax = 10
		elif secondaryTitle == "建筑装修":
			pageMax = 11

	if "Baidu" in sourceWeb:
		finalName = "data_detail_Baidu_" + changeName + "(" + timestamp + ").txt"

		getDataListByBaiduUrl("data_save.txt", keyword, pageMax)
		getRawDataByBaiduUrl("data_save.txt", "data_detail000.txt", primaryTitle, secondaryTitle)
		getDetailData(sourceWeb, "data_detail000.txt", finalName)
	elif "City8090" in sourceWeb:
		url = 'http://life.city8090.com/shenzhen/' + changeName + '/more'
		finalName = "data_detail_City8090_" + changeName + "(" + timestamp + ").txt"

		getDataListByCity8090Url("data_save.txt", url)
		getRawDataByCity8090Url("data_save.txt", "data_detail.txt", primaryTitle, secondaryTitle)
		getDetailData(sourceWeb, "data_detail.txt", finalName)


