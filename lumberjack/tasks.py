from lumberjack.celery import app
from lumberjack.collector import collecte_metas, collect_posts
from lumberjack.strategy import Datastore


@app.task
def collect_meta_task(bundle):
    forum, param = bundle
    return collecte_metas(
        forum, **param, callback=Datastore.upsert_metas_if_newer)


@app.task
def collect_all_metas_task(forum):
    return collecte_metas(
        forum, callback=Datastore.insert_metas)


@app.task
def collect_posts_task(forum):
    db = Datastore()
    tasks = [
        {'_id': m['_id'], 'id': m['id'], 'commentCount': m['commentCount']}
        for m in db.find_pending_metas(forum)
    ]
    for task, post in zip(tasks, collect_posts(tasks)):
        db.finish_pending_meta(forum, task)
        db.save(post)
