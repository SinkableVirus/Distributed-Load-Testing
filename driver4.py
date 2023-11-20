from DriverClass import DriverNode

try:
    driver = DriverNode()
    driver.scheduler1.add_job(driver.sendHeatbeat, 'interval', seconds = 1)
    while True:
        driver.listen()
except KeyboardInterrupt:
    print("Driver Stopped")