import logging

from celery import Celery

from dcard import Dcard
from lumberjack.datastore import Datastore


BROKER_URL = 'redis://localhost:6379/0'
CELERY_RESULT_BACKEND = 'redis://localhost:6379/1'

app = Celery(
    'lumberjack',
    broker=BROKER_URL,
    backend=CELERY_RESULT_BACKEND,
    include=['lumberjack.tasks']
)

logger = logging.getLogger(__name__)
dcard = Dcard()
db = Datastore()


@app.task
def collect_meta_task(bundle):
    forum, param = bundle
    param.setdefault('num', dcard.forums.infinite_page)
    return dcard.forums(forum).get_metas(
        **param,
        callback=lambda metas, forum=forum:
        db.upsert_metas_if_newer(metas, forum))


@app.task
def collect_all_metas_task(forum):
    return dcard.forums(forum).get_metas(
        num=dcard.forums.infinite_page,
        callback=lambda metas, forum=forum: db.insert_metas(metas, forum))


@app.task
def collect_posts_task(forum):
    tasks = [
        {'_id': m['_id'], 'id': m['id'], 'commentCount': m['commentCount']}
        for m in db.find_pending_metas(forum)
    ]
    for task, post in zip(tasks, dcard.posts(tasks).get()):
        try:
            db.save(post)
            db.finish_pending_meta(forum, task)
        except Exception as e:
            logger.error('when fectch and save post', exc_info=True)
            logger.error(e, task, post)
