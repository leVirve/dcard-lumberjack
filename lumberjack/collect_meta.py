import time
import logging

from dcard import Dcard

from lumberjack.strategy import DBStrategy


logger = logging.getLogger(__name__)


def collecter(
        name,
        boundary_num=None,
        before_id=None,
        boundary_date=None,
        callback=None):

    dcard = Dcard()

    s = time.time()
    result = dcard.forums(name).get_metas(
            num=boundary_num if boundary_num else dcard.forums.infinite_page,
            before=before_id if before_id else None,
            timebound=boundary_date.isoformat() if boundary_date else '',
            callback=lambda metas, name=name: callback(metas, name)
        )

    logger.debug('<%s> used %.05f sec.', name, time.time() - s)
    return result


def collect_all(forum):
    return collecter(forum, callback=DBStrategy.insert_metas)


def collect(bundle):
    forum, param = bundle
    return collecter(forum, **param, callback=DBStrategy.upsert_metas)
