import hashlib
import os
from datetime import datetime

from .logger import log

class md5summer:
    def __init__(self, md5sum_file: str):
        self.md5sum_file = md5sum_file
        self.md5sums = {}

        if not os.path.exists(self.md5sum_file):
            fp = open(self.md5sum_file, 'w')
            fp.close() # Making an empty file
        self.md5sum_mdate = datetime.fromtimestamp(os.path.getmtime(md5sum_file))

        with open(md5sum_file, 'r') as fp:
            for line in fp.readlines():
                d = line.strip().split()
                self.md5sums[d[1]] = d[0] # {"filename": "md5sum"}

    def get_md5sum(self, filename: str) -> str:
        if filename in self.md5sums.keys():
            if self.md5sum_mdate > datetime.fromtimestamp(os.path.getmtime(filename)):
                log.info(f'Already calculated MD5SUM previously ({self.md5sums[filename]})')
                return self.md5sums[filename]
        else:
            with open(filename, 'rb') as fp:
                file_hash = hashlib.md5()
                while chunk := fp.read(8192):
                    file_hash.update(chunk)
            log.debug(f'Calculated md5 {file_hash.hexdigest()}')
            with open(self.md5sum_file, 'a') as fp:
                fp.write(file_hash.hexdigest() + ' ' + filename + '\n')
            return file_hash.hexdigest()
