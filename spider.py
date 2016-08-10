import time
import logging
from multiprocessing.dummy import Pool

from dcard import Dcard


logger = logging.getLogger('lumberjack')


if __name__ == '__main__':
    s = time.time()

    dcard = Dcard()
    forums = dcard.forums.get(no_school=True)

    thread_pool = Pool(processes=2)
    result = thread_pool.map_async(
        '''collect_metas''',
        [forum['alias'] for forum in forums])
    result.get()

    logger.info('Total Work: {:.05} sec'.format(time.time() - s))
