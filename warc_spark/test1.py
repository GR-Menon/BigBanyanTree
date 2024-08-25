import argparse
from time import sleep

parser = argparse.ArgumentParser()
parser.add_argument("--text", help="some text to print.")
args = parser.parse_args()

sleep(7)
print(args.text, " from test1.py")
