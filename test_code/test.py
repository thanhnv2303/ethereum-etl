import os

for root, dirs, files in os.walk("../artifacts/event-abi"):
    for filename in files:
        print(filename)
        print(type(filename))