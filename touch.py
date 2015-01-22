#!/usr/bin/python3

import os
import time
import subprocess

newlines = []
try:
    file = open('/tmp/test', 'r')
except IOError:
    print('open failed...')
# read the data from the last position
print(file)
for newline in file:
    newlines.append(newline)
print(newlines)
file.close()

print(newlines)

# traverse root directory, and list directories as dirs and files as files
def touch_files(init_dir):
    print('walking...1')
    for root, dirs, files in os.walk(init_dir):
        print('walking...2')
        for file in files:
            print('walking...3')
            file_path = os.path.join(root, file)

            subprocess.call(['touch', file_path])
            print(str(time.ctime(os.path.getctime(file_path))) + '\t' + file_path)

            time.sleep(300)

        for direc in dirs:
            touch_files(direc)


# print('walking...0')
# touch_files("/tmp/test/")
