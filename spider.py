import logging
import logging.config
import yaml
import datetime

from dcard import Dcard
from lumberjack.tasks import (
    collect_meta_task, collect_all_metas_task, collect_posts_task)


with open('logger.yaml', 'rt') as f:
    config = yaml.load(f.read())
logging.config.dictConfig(config)


logger = logging.getLogger('lumberjack')


dcard = Dcard()
forums = dcard.forums.get(no_school=True)


def get_all_metas(forum_name):
    ''' Run this once, and all metas will insert into database. '''
    return collect_all_metas_task.delay(forum_name)


def update_metas_recently(forum_name):
    time_limitation = datetime.datetime.utcnow() - datetime.timedelta(days=30)
    bundle = (
        forum_name,
        {'timebound': time_limitation.isoformat()}
    )
    return collect_meta_task.delay(bundle)


def get_new_posts(forum_name):
    return collect_posts_task.delay(forum_name)


if __name__ == '__main__':
    forum = 'pokemon'

    result = update_metas_recently(forum)
    print(result.get())

    r2 = get_new_posts(forum)
    print(r2)
