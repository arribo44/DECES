from random import randint

from tqdm import tqdm, trange
import time
import random

# Progress bar 1: Default settings
for i in tqdm(range(300)):
    time.sleep(0.01)

# Progress bar 2: Customized bar format and color
for i in tqdm(range(300), bar_format='[{elapsed}<{remaining}] {n_fmt}/{total_fmt} | {l_bar}{bar} {rate_fmt}{postfix}',
              colour='yellow'):
    time.sleep(0.01)

# Progress bar 3: Customized bar format and color, leave=False
for i in tqdm(range(300), bar_format='[{elapsed}<{remaining}] {n_fmt}/{total_fmt} | {l_bar}{bar} {rate_fmt}{postfix}',
              colour='red', leave=False):
    time.sleep(0.01)


for i in trange(3, desc='outer loop'):
    for j in trange(2, desc='middle loop'):
        for k in trange(6, desc='inner loop'):
            time.sleep(0.05)