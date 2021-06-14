#!/usr/bin/python3
import getopt

import sys
import subprocess


def versiontuple(v) -> tuple:
    return tuple(map(int, (v.split("."))))


def main(argv):
    print('This is script datastore_driver.py')
    print(sys.version_info)

    actual_version = '{}.{}.{}'.format(sys.version_info[0], sys.version_info[1], sys.version_info[2])
    if versiontuple(actual_version) < versiontuple("3.8.2"):
        raise Exception('Python version is {}, required minimum 3.8.2'.format(actual_version))

    batch_file = ''
    try:
        opts, args = getopt.getopt(argv, "hf:", ["file="])
    except getopt.GetoptError:
        print('datastore_driver.py -f <batch_file>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('Reads a file with commands and executes them sequentially\n' \
                  'datastore_driver.py -f <batch_file>')
            sys.exit()
        elif opt in ("-f", "--file"):
            batch_file = arg

    insert_dash_line()
    print('BATCH MODE - READING FROM FILE ' + batch_file)
    insert_dash_line()

    file = open(batch_file, 'r')
    Lines = file.readlines()

    count = 0
    for line in Lines:
        count += 1
        command = line.strip()
        print("Line{}: {}".format(count, command))
        insert_dash_line()
        if len(command) > 0 and not command[0] == '#':
            command_list = command.split(" ")
            sub = subprocess.run(command_list)  # Safer run, bypasses the shell
            if sub.returncode != 0:
                print('Failed with ERROR!')
                exit(-1)


def insert_dash_line():
    print("-" * 65)


if __name__ == "__main__":
    main(sys.argv[1:])
