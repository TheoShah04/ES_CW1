import sqlite3
from picamera2 import Picamera2
import cv2
from pyzbar import pyzbar
import time

picam2 = Picamera2()
picam2.configure(picam2.create_still_configuration(main={"size": (1080, 1080)}))
picam2.start()
DB_FILE = "/home/pi/processed.db"
conn = sqlite3.connect(DB_FILE)
cur = conn.cursor()
time.sleep(1)

try:
    while True:
        frame = picam2.capture_array()
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        barcodes = pyzbar.decode(gray)
        # rotated = cv2.rotate(frame, cv2.ROTATE_90_COUNTERCLOCKWISE)
        cv2.imwrite("/home/pi/ES_CW1/barcode_scripts/colour.png", frame)
        cv2.imwrite("/home/pi/ES_CW1/barcode_scripts/grayscale.png", gray)

        if len(barcodes) == 0:
            print("No barcodes detected")
        else:
            for barcode in barcodes:
                print(barcode.type, barcode.data.decode("utf-8"))
                cur.execute("SELECT name FROM products WHERE code=?", (barcode,))
                result = cur.fetchone()
                if result:
                    print("Product name:", result[0])
                else:
                    print("Barcode not found.")
        time.sleep(0.5)

except KeyboardInterrupt:
    print("Stopping loop \n")

finally:
    conn.close()
    picam2.stop()