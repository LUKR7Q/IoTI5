"""서브모터 로봇팔

GPIO 아래부터 (7,11,13,15,16)
GROUND 14

(기본 세팅 자세)
gpio 7번 유지
gpio 11번 45도
gpio 13번 90도
gpio 15번 80도
gpio 16번 180도

---------------------------------------------------------------------------------------"""
import RPi.GPIO as GPIO
import threading
import time

# GPIO 핀 번호 설정
servo_pins = [11, 13, 15]

# GPIO 핀 번호 모드 설정
GPIO.setmode(GPIO.BOARD)

# 서보 모터 핀 설정
for pin in servo_pins:
    GPIO.setup(pin, GPIO.OUT)

# PWM 객체 생성 (주파수: 50Hz)
pwms = [GPIO.PWM(pin, 50) for pin in servo_pins]

# PWM 신호 시작
for pwm in pwms:
    pwm.start(0)

def control_servo(pin, duty_cycle):
    while True:
        pwm = pwms[pin]
        pwm.ChangeDutyCycle(duty_cycle)
        time.sleep(1)

try:
    threads = []

    # 서보 모터를 각각의 스레드에서 제어
    for idx, pin in enumerate(servo_pins):
        duty_cycle = 9.5  # 초기 각도 설정
        thread = threading.Thread(target=control_servo, args=(idx, duty_cycle))
        threads.append(thread)
        thread.start()

    # 프로그램이 계속 실행되도록 유지
    while True:
        pass

except KeyboardInterrupt:
    # 사용자가 Ctrl+C를 누르면 종료
    pass

finally:
    # 정리 작업
    for pwm in pwms:
        pwm.stop()
    GPIO.cleanup()

    # 모든 스레드가 종료될 때까지 대기
    for thread in threads:
        thread.join()

        