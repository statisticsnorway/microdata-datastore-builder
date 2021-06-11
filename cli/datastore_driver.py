#!/usr/bin/python3
import sys
import subprocess

print('Dette er datastore_driver.py')
print('===========================')
print(sys.version_info)


def versiontuple(v):
    return tuple(map(int, (v.split("."))))


actual_version = '{}.{}.{}'.format(sys.version_info[0], sys.version_info[1], sys.version_info[2])
if versiontuple(actual_version) < versiontuple("3.8.2"):
    raise Exception('Python version {}, must run on minimum 3.8.2'.format(actual_version))

print('===========================')

print('----------------------------------------')
print('BATCH MODE - READING FROM FILE batch.txt')
print('----------------------------------------')

file = open('batch.txt', 'r')
Lines = file.readlines()

count = 0
for line in Lines:
    count += 1
    command = line.strip()
    print("Line{}: {}".format(count, command))
    print('---------------------------------')
    if len(command) > 0 and not command[0] == '#':
        command_list = command.split(" ")
        sub = subprocess.run(command_list)  # Safer run, bypasses the shell
        if sub.returncode != 0:
            print('Failed with ERROR!')
            exit(-1)
