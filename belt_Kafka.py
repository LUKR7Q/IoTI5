import RPi.GPIO as GPIO
import time
from kafka import KafkaProducer, KafkaConsumer
import json

# GPIO pin setup for the relay
GPIO.setmode(GPIO.BCM)
relay_pin = 17  # BCM 17
GPIO.setup(relay_pin, GPIO.OUT)
GPIO.setwarnings(False)

# kafka 
bootstrap_servers = ['10.10.10.111:9092']

producer = KafkaProducer(
	acks=0,
	compression_type='gzip', 
	bootstrap_servers=bootstrap_servers,
	value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# GPIO pin setup for the ultrasonic sensor
TRIG = 23
ECHO = 24
print("start")

GPIO.setup(TRIG, GPIO.OUT)
GPIO.setup(ECHO, GPIO.IN)

GPIO.output(TRIG, False)
print("init")
time.sleep(2)

try:
    while True:
        GPIO.output(TRIG, True)
        time.sleep(0.00001)
        GPIO.output(TRIG, False)

        while GPIO.input(ECHO) == 0:
            start = time.time()

        while GPIO.input(ECHO) == 1:
            stop = time.time()

        check_time = stop - start
        distance = check_time * 34300 / 2
        print("Distance: %.1f cm" % distance)

        if distance <= 16:  # Check if the distance is 20cm or less
            # Turn the motor off
            GPIO.output(relay_pin, GPIO.HIGH)
            print("Motor is OFF (Distance is too close)")
            
            data = {
			"IoT": "Belt",
            "Status" : "on",
			"Command": {
                "Target" : "RobotArm",
                "Status" : "on"
                }
            }
            producer.send('toIot', value=data)
            
            time.sleep(3)
            GPIO.output(relay_pin, GPIO.LOW)
            time.sleep(3)
        else:
            # Turn the motor on
            GPIO.output(relay_pin, GPIO.LOW)
            print("Motor is ON (Distance is safe)")

        time.sleep(0.4)

except KeyboardInterrupt:
    print("quit")
    GPIO.cleanup()
