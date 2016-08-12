import time
import logging

from dcard import Dcard


logger = logging.getLogger(__name__)


def collect_metas(
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

    logger.info('<%s> used %.05f sec.', name, time.time() - s)
    return result


def collect_posts(metas):
    dcard = Dcard()
    return dcard.posts(metas).get()
