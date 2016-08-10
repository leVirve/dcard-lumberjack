import time
import logging
import datetime
from multiprocessing.dummy import Pool

from dcard import Dcard
from lumberjack.collect_meta import collect, collect_all


logger = logging.getLogger('lumberjack')

# Setup Handler
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console = logging.StreamHandler()
console.setLevel(logging.DEBUG)
console.setFormatter(formatter)

# Setup Logger
logger.addHandler(console)
logger.setLevel(logging.DEBUG)


dcard = Dcard()
forums = dcard.forums.get(no_school=True)


def brute(forums):
    with Pool(processes=2) as pool:
        result = pool.map(collect, forums)
    return result


def get_all_metas(forum_name):
    ''' Run this once, and all metas will insert into database. '''
    return collect_all(forum_name)


def get_metas_in_one_weeks(forum_name):
    ''' Run this every week, and metas will upsert into database. '''
    time_limitation = datetime.datetime.utcnow() - datetime.timedelta(weeks=1)
    bundle = (
        forum_name,
        {'boundary_date': time_limitation}
    )
    return collect(bundle)


if __name__ == '__main__':
    s = time.time()

    result = get_metas_in_one_weeks('pokemon')
    print(result)

    logger.info('Total Work: {:.05} sec'.format(time.time() - s))
