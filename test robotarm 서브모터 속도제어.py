import RPi.GPIO as GPIO
import time

# GPIO 핀 번호 설정
servo_pin = 16

# GPIO 초기화
GPIO.setmode(GPIO.BOARD)
GPIO.setup(servo_pin, GPIO.OUT)

# PWM 객체 생성
pwm = GPIO.PWM(servo_pin, 50)  # PWM 주파수 50Hz 설정

# Duty cycle 변환 함수
def angle_to_duty_cycle(angle):
    return 2.5 + (angle / 18)

try:
    command = input("Enter 'on' to start, 'off' to stop: ")

    if command == 'on':
        while True:
            # 0도에서 180도로 이동
            for angle in range(0, 181, 1):
                duty_cycle = angle_to_duty_cycle(angle)
                pwm.start(duty_cycle)
                time.sleep(2/180)  # 2초 안에 움직이도록 조절

            time.sleep(3)  # 3초 대기

            # 180도에서 0도로 이동
            for angle in range(180, -1, -1):
                duty_cycle = angle_to_duty_cycle(angle)
                pwm.start(duty_cycle)
                time.sleep(2/180)  # 2초 안에 움직이도록 조절

            time.sleep(3)  # 3초 대기

    elif command == 'off':
        pass

except KeyboardInterrupt:
    pass

# 프로그램 종료 시 GPIO 정리
pwm.stop()
GPIO.cleanup()
