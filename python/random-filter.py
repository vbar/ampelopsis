#!/usr/bin/python3

import argparse
import csv
import random
import sys

class Pipe:
    def run(self, file_names):
        for fname in file_names:
            with open(fname) as f:
                reader = csv.reader(f, delimiter="\t")
                for row in reader:
                    self.handle_line(row)


class CountingPipe(Pipe):
    def __init__(self):
        self.total = 0

    def handle_line(self, row):
        self.total += 1


class CopyingPipe(Pipe):
    def __init__(self, writer):
        self.writer = writer

    def handle_line(self, row):
        self.writer.writerow(row)


class FilteringPipe(Pipe):
    def __init__(self, writer, threshold):
        random.seed()
        self.writer = writer
        self.threshold = threshold

    def handle_line(self, row):
        if random.random() <= self.threshold:
            self.writer.writerow(row)


def main():
    parser = argparse.ArgumentParser(description='Randomly reduce input file lines')
    parser.add_argument('--target', nargs='?', default=1000, type=int)
    parser.add_argument('file', nargs='+', help='input file name')
    args = parser.parse_args()

    if args.target <= 0:
        raise Exception("target must be positive")

    counter = CountingPipe()
    counter.run(args.file)
    total = counter.total
    if not total:
        print("no input lines", file=sys.stderr)
    else:
        writer = csv.writer(sys.stdout, delimiter=',')
        if total <= args.target:
            cp = CopyingPipe(writer)
            cp.run(args.file)
            print("no filtering - input already less than %d lines" % args.target, file=sys.stderr)
        else:
            flt = FilteringPipe(writer, args.target / total)
            flt.run(args.file)


if __name__ == "__main__":
    main()
