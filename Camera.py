from kafka import KafkaProducer, KafkaConsumer
from json import dumps
import time
import cv2
import base64
import requests
import json
import threading
bootstrap_servers = ['KAFKA_URL']

producer = KafkaProducer(
	acks=0,
	compression_type='gzip', 
	bootstrap_servers=bootstrap_servers,
	value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

consumer = KafkaConsumer(
	'toIot',
	bootstrap_servers=bootstrap_servers,
	#auto_offset_reset='earliest',
	enable_auto_commit=True,
)
status = True
def send_to_web_periodically():
	while True:
		data = {
			"IoT": "Cam",
			"Status": "on" if status else "off"
		}

		producer.send('toWeb', value=data)

		time.sleep(5)


def run_consumer():
	global status
	for message in consumer:
		print('Received message: {}'.format(message.value))
		message_json = message.value.decode('utf8').replace("'", '"')
		message_json = json.loads(message_json)
		if message_json["command"]["target"] == "Cam":
			if message_json["command"]["status"] == 'on':
				status = True
			elif message_json["command"]["status"] == 'off':
				status = False

consumer_thread = threading.Thread(target=run_consumer)
consumer_thread.daemon = True
consumer_thread.start()

producer_thread = threading.Thread(target=send_to_web_periodically)
producer_thread.daemon = True
producer_thread.start()


cap = cv2.VideoCapture(0)

headers = {
    'Content-Type': 'application/json'
}
url = "AIServerURL/upload"



try:
	while True:
		if status:   
			ret, frame = cap.read()
	
			if ret:
				img = cv2.resize(frame, (740,740))		
				img = img[:640,:640]
				img = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
				
				retval, buffer = cv2.imencode('.jpg', img)
				base64_image = base64.b64encode(buffer).decode('utf-8')
				data1 = {
					"name" : "Cam1",
					"Frame": base64_image,
				}
				try:
					response = requests.post(url, json=data1, headers=headers)
					print(response.status_code)
				except:
					print("request error")
			else:
				print(f"status : {status}")
		
		else:
			print("can't read image.")
except KeyboardInterrupt:
    pass

