from lumberjack.celery import app
from lumberjack.collect_meta import collecter, cposts
from lumberjack.strategy import DBStrategy, Datastore


@app.task
def collect(bundle):
    forum, param = bundle
    return collecter(forum, **param, callback=DBStrategy.upsert_metas_if_newer)


@app.task
def collect_all(forum):
    return collecter(forum, callback=DBStrategy.insert_metas)


@app.task
def collect_posts(forum):
    db = Datastore()
    tasks = [
        {'_id': m['_id'], 'id': m['id'], 'commentCount': m['commentCount']}
        for m in DBStrategy.find_pending_metas(forum)
    ]
    for task, post in zip(tasks, cposts(tasks)):
        DBStrategy.change_pending_meta(forum, task)
        db.save(post)
