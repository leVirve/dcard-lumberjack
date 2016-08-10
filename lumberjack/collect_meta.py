

from lumberjack.crawler import crawl
from lumberjack.strategy import DBStrategy


def collect_all(forum):
    return crawl(forum, callback=DBStrategy.insert_metas)


def collect(bundle):
    forum, param = bundle
    return crawl(forum, **param, callback=DBStrategy.upsert_metas)
