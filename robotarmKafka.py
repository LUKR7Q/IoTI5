import RPi.GPIO as GPIO
import time
from kafka import KafkaProducer, KafkaConsumer
import json

GPIO.setwarnings(False)
# GPIO 핀 번호 설정
servo_pin_4 = 4
servo_pin_27 = 27
servo_pin_23 = 23
servo_pin_17 = 17
servo_pin_22 = 22

# GPIO 핀 모드 설정
GPIO.setmode(GPIO.BCM)
GPIO.setup(servo_pin_4, GPIO.OUT)
GPIO.setup(servo_pin_27, GPIO.OUT)
GPIO.setup(servo_pin_23, GPIO.OUT)
GPIO.setup(servo_pin_17, GPIO.OUT)
GPIO.setup(servo_pin_22, GPIO.OUT)

# PWM 초기화
pwm_4 = GPIO.PWM(servo_pin_4, 50)  # PWM 주파수 설정 (50 Hz)
pwm_27 = GPIO.PWM(servo_pin_27, 50)
pwm_23 = GPIO.PWM(servo_pin_23, 50)
pwm_17 = GPIO.PWM(servo_pin_17, 50)
pwm_22 = GPIO.PWM(servo_pin_22, 50)

# 서보 모터를 초기 위치로 이동
pwm_4.start(0)  # PWM 신호 시작 (Duty Cycle 0%)
pwm_27.start(0)
pwm_23.start(0)
pwm_17.start(0)
pwm_22.start(0)


def run_robot_arm():
    # play robotArms
    pwm_22.ChangeDutyCycle(2.5)
    time.sleep(1)
    pwm_17.ChangeDutyCycle(4.5)
    time.sleep(1)
    # 1. gpio23을 2.5로 고정
    pwm_23.ChangeDutyCycle(2.5)
    time.sleep(1)
    # 2. gpio17을 3.5로 고정
    pwm_27.ChangeDutyCycle(10.5)
    time.sleep(1)
    # 3. gpio4를 7.5로 고정
    pwm_4.ChangeDutyCycle(4.5)
    time.sleep(1)
    # 4. gpio17을 5.5로 고정
    pwm_27.ChangeDutyCycle(12.0)
    time.sleep(1)
    # 5. gpio23을 12.5로 고정
    pwm_23.ChangeDutyCycle(12.5)
    time.sleep(1)
    # 6. gpio17을 5.5로 고정
    pwm_27.ChangeDutyCycle(9.5)
    time.sleep(1)
    # 7. gpio4를 4.5로 고정
    pwm_4.ChangeDutyCycle(12.5)
    time.sleep(1)
    # 8. gpio17을 3.5로 고정
    pwm_27.ChangeDutyCycle(11.0)
    time.sleep(1)
    # 9. gpio23을 2.5로 고정
    pwm_23.ChangeDutyCycle(2.5)
    time.sleep(1)


# kafka
consumer = KafkaConsumer(
    'toIot', bootstrap_servers='10.10.10.111:9092',
    enable_auto_commit=True,
)

while True:
    for message in consumer:
        message_value = json.loads(message.value.decode('utf-8'))
        print(message_value)
        target_value = message_value.get('Command', {}).get('Target')
        status_value = message_value.get('Command', {}).get('Status')
        # print(status_value)
        print(target_value)
        if target_value == 'RobotArm':
            if status_value == 'on':
                time.sleep(3.3)
                run_robot_arm()
            else:
                print('off off off')

# PWM 정지 및 GPIO 해제
pwm_4.stop()
pwm_17.stop()
pwm_23.stop()
GPIO.cleanup()



