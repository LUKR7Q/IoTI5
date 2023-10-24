
import RPi.GPIO as GPIO
import time

# GPIO 핀 번호 설정
servo_pin = 16

# GPIO 초기 설정
GPIO.setmode(GPIO.BOARD)
GPIO.setup(servo_pin, GPIO.OUT)

# PWM 초기화
pwm = GPIO.PWM(servo_pin, 50)
pwm.start(0)

def set_angle(angle):
    duty = angle / 18 + 2
    GPIO.output(servo_pin, True)
    pwm.ChangeDutyCycle(duty)
    #time.sleep(1)
    #GPIO.output(servo_pin, False)
    #pwm.ChangeDutyCycle(0)

try:
    running = True

    while running:
        command = 'on'
        #command = input("Enter 'on' to start or 'off' to stop: ")

        if command == 'on':
            while running:
                # 0도로 이동
                set_angle(0)
                time.sleep(1)

                # 180도로 이동
                set_angle(180)
                time.sleep(3)
        elif command == 'off':
            running = False
            break

except KeyboardInterrupt:
    # 프로그램 종료 시 GPIO 정리
    pwm.stop()
    GPIO.cleanup()
