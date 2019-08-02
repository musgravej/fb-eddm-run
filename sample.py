import time
from datetime import datetime as dt
import os
import sys
import platform

def main():
    datestring = dt.strftime(dt.now(), "%Y/%m/%d %H:%M:%S")
    print("Hello world {}".format(datestring))

    search_path = os.path.join(os.path.abspath(os.sep),"media","Print","FTPfiles","LocalUser","FB-EDDM")

    list_files = [f for f in os.listdir(search_path)]
    print(list_files)
    print(platform.system())


if __name__ == '__main__':
    main()