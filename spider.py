import logging
import datetime

from dcard import Dcard
from lumberjack.tasks import (
    collect_meta_task, collect_all_metas_task, collect_posts_task)


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
    return collect_all_metas_task.delay(forum_name)


def update_metas_in_one_weeks(forum_name):
    ''' Run this every day, and metas will upsert into database. '''
    time_limitation = datetime.datetime.utcnow() - datetime.timedelta(weeks=1)
    bundle = (
        forum_name,
        {'timebound': time_limitation.isoformat()}
    )
    return collect_meta_task.delay(bundle)


def get_new_posts(forum_name):
    return collect_posts_task.delay(forum_name)


if __name__ == '__main__':
    forum = 'pokemon'

    result = update_metas_in_one_weeks(forum)
    print(result.get())

    r2 = get_new_posts(forum)
    print(r2)
