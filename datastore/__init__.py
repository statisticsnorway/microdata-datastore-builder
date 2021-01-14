import os
#import sys

#sys.path.append(os.path.dirname(__file__)
#print(sys.path)

def resolve_filename(filename):
    return os.path.join(os.path.dirname(__file__), filename)
