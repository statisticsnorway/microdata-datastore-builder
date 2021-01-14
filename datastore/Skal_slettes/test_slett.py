start = 123

if start in(None, ""):
    print(start)
else:
    print("None")


import random

a = []
for i in range(1,10000):
    a.append(random.randint(0, 999))
a.sort()
print(a[0])
print(a[-1])