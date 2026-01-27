from picamera2 import Picamera2
import cv2
from pyzbar import pyzbar
import time

picam2 = Picamera2()
picam2.configure(picam2.create_still_configuration(main={"size": (640, 640)}))
picam2.start()
time.sleep(1)

try:
    while True:
        frame = picam2.capture_array()
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        barcodes = pyzbar.decode(gray)
        rotated = cv2.rotate(frame, cv2.ROTATE_90_COUNTERCLOCKWISE)
        cv2.imwrite("/home/pi/ES_CW1/colour.png", rotated)
        cv2.imwrite("/home/pi/ES_CW1/grayscale.png", gray)

        if len(barcodes) == 0:
            print("No barcodes detected")
        else:
            for barcode in barcodes:
                print(barcode.type, barcode.data.decode("utf-8"))
        time.sleep(1)

except KeyboardInterrupt:
    print("Stopping loop \n")

finally:
    picam2.stop()