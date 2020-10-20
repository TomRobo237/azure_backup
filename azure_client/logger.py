import logging
import os
import sys

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

stream_handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('Thread: %(thread)d - %(asctime)s : %(levelname)s: %(message)s')
stream_handler.setFormatter(formatter)

log.addHandler(stream_handler)

