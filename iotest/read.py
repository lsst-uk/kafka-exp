# Naive IO test that reads n 500K files to disk

from time import time

prefix = "iotest_"
n = 10000

start = time()
for i in range(n):
    f = open(prefix+str(i), 'r')
    data = f.read()
    f.close
end = time()

print("Read {:d} files of 500K in {:.2f} s".format(n, (end-start)))
print("{:.2f} files/s at {:.2f} MB/s".format(n/(end-start), (n*0.5/(end-start))))

