import time
import logging

from dcard import Dcard
from lumberjack.celery import app
from lumberjack.datastore import Datastore


logger = logging.getLogger(__name__)
dcard = Dcard()


@app.task
def collect_meta_task(bundle):
    forum, param = bundle
    param.setdefault('num', dcard.forums.infinite_page)
    return dcard.forums(forum) \
                .get_metas(**param, callback=Datastore.upsert_metas_if_newer)


@app.task
def collect_all_metas_task(forum):
    return dcard.forums(forum).get_metas(
        num=dcard.forums.infinite_page,
        callback=Datastore.insert_metas)


@app.task
def collect_posts_task(forum):
    db = Datastore()
    tasks = [
        {'_id': m['_id'], 'id': m['id'], 'commentCount': m['commentCount']}
        for m in db.find_pending_metas(forum)
    ]
    for task, post in zip(tasks, dcard.posts(tasks).get()):
        db.finish_pending_meta(forum, task)
        db.save(post)
