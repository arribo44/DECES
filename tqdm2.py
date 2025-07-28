import concurrent.futures
import random

from tqdm import tqdm
from tqdm.contrib.concurrent import process_map
import time

def process_data(data):
    for i in tqdm(range(data['iRange']), desc=f"Processing {data['name']}"):
        # Process data
        time.sleep(data['iWait'])


if __name__ == '__main__':
    ledataset = [
            {'name': 'dataset1', 'iWait': round(random.uniform(0.01, 0.09), 2), 'iRange': random.randint(100, 1000)},
            {'name': 'dataset2', 'iWait': round(random.uniform(0.01, 0.09), 2), 'iRange': random.randint(100, 1000)},
            {'name': 'dataset3', 'iWait': round(random.uniform(0.01, 0.09), 2), 'iRange': random.randint(100, 1000)},
            {'name': 'dataset4', 'iWait': round(random.uniform(0.01, 0.09), 2), 'iRange': random.randint(100, 1000)},
            {'name': 'dataset5', 'iWait': round(random.uniform(0.01, 0.09), 2), 'iRange': random.randint(100, 1000)},
            {'name': 'dataset6', 'iWait': round(random.uniform(0.01, 0.09), 2), 'iRange': random.randint(100, 1000)},
        ]
    with concurrent.futures.ProcessPoolExecutor() as executor:
        results = process_map(process_data, ledataset)
