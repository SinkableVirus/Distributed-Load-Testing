from DriverClass import DriverNode

try:
    driver = DriverNode()
    driver.listen()
except KeyboardInterrupt:
    print("Driver Stopped")