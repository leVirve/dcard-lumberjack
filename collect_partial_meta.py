import time
import datetime

from dcard import Dcard


if __name__ == '__main__':
    s = time.time()

    boundary_date = datetime.datetime.utcnow() - datetime.timedelta(hours=1)

    dcard = Dcard()
    name = 'bg'

    metas = dcard.forums(name).get_metas(
                num=dcard.forums.infinite_page,
                timebound=boundary_date.isoformat()
            )
    print(name, len(metas))

    name = 'freshman'
    metas = dcard.forums(name).get_metas(
                num=dcard.forums.infinite_page,
            )
    print(name, len(metas))

    print('{:.05} sec'.format(time.time() - s))
