import os
import shutil
import random
import time

TEST_DATA_TEMP_DIR = '/root/tmp/'
TEST_DATA_DIR = '/root/tmp/testdata/'

ACTION_DEF = ['login', 'logout', 'purchase']
DISTRICT_DEF = ['fujian', 'beijing', 'shanghai', 'guangzhou']
JSON_LINE_PATTERN = '{{"eventTime":{}, "action":"{}", "district":"{}"}}\n'

def test_setUp():
    if os.path.exists(TEST_DATA_DIR):
        shutil.rmtree(TEST_DATA_DIR, ignore_errors=True)
    os.mkdir(TEST_DATA_DIR)

def test_tearDown():
    if os.path.exists(TEST_DATA_DIR):
        shutil.rmtree(TEST_DATA_DIR, ignore_errors=True)

def write_and_move(fileName, data):
    with open(TEST_DATA_TEMP_DIR+fileName, "wt", encoding="utf-8") as f:
        f.write(data)
    shutil.move(TEST_DATA_TEMP_DIR+fileName, TEST_DATA_DIR+fileName)

if __name__ == '__main__':
    test_setUp()

    for i in range(1000):
        fileName = 'e-mail-{}.json'.format(i)

        content = ''
        rndcount = list(range(100))
        random.shuffle(rndcount)
        for _ in rndcount:
            content += JSON_LINE_PATTERN.format(
                str(int(time.time())),
                random.choice(ACTION_DEF),
                random.choice(DISTRICT_DEF)
            )
        write_and_move(fileName, content)

        time.sleep(2)

    test_tearDown()
