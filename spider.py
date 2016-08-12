import logging
import datetime

from dcard import Dcard
from lumberjack.tasks import collect, collect_all, collect_posts


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


def get_all_metas(forum_name):
    ''' Run this once, and all metas will insert into database. '''
    return collect_all.delay(forum_name)


def get_metas_in_one_weeks(forum_name):
    ''' Run this every week, and metas will upsert into database. '''
    time_limitation = datetime.datetime.utcnow() - datetime.timedelta(hours=1)
    bundle = (
        forum_name,
        {'boundary_date': time_limitation}
    )
    return collect.delay(bundle)


if __name__ == '__main__':
    get_metas_in_one_weeks('pokemon')
    collect_posts('pokemon')
