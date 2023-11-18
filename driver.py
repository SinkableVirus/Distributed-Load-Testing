import threading
from DriverClass import DriverNode

# driver = DriverNode()
# driver.startTest(0.5)

# driver.displayMetrics()


drivers = []
threads = []
numberOfDrivers = 8

for i in range(numberOfDrivers):
    driver = DriverNode()
    drivers.append(driver)
    thread = threading.Thread(target = driver.listen)
    threads.append(thread)

for i in range(numberOfDrivers):
    threads[i].start()

for i in range(numberOfDrivers):
    threads[i].join()

for i in range(numberOfDrivers):
    drivers[i].displayMetrics()