import time
import logging
import datetime

from dcard import Dcard
from lumberjack.crawler import crawl
from lumberjack.strategy import DBStrategy


logger = logging.getLogger('lumberjack')

# Setup Handler
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console = logging.StreamHandler()
console.setLevel(logging.DEBUG)
console.setFormatter(formatter)

# Setup Logger
logger.addHandler(console)
logger.setLevel(logging.DEBUG)


def main():
    dcard = Dcard()
    forums = dcard.forums.get(no_school=True)
    forums = [forum['alias'] for forum in forums]

    boundary_date = datetime.datetime.utcnow() - datetime.timedelta(hours=1)

    forum = forums[0]

    # brand new crawl
    crawl(forum, callback=DBStrategy.insert_metas)

    # crawl recently
    crawl(forum, boundary_date=boundary_date, callback=DBStrategy.upsert_metas)

    # crawl before
    crawl(forum, before=54051, callback=DBStrategy.upsert_metas)


if __name__ == '__main__':
    s = time.time()
    main()
    logger.info('Total Work: {:.05} sec'.format(time.time() - s))
