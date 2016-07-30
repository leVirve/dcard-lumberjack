import time
import datetime

from dcard import Dcard


def hack_forums_get_meta_function(dcard):

    target_date = datetime.datetime.utcnow() - datetime.timedelta(days=1)

    def get_paged_metas(self, pages, sort):
        params = {'popular': False} if sort == 'new' else {}

        for page in range(pages):

            data = self.client.get(self.posts_meta_url, params=params)

            if len(data) == 0:
                logger.warning('[%s] 已到最末頁，第%d頁!' % (self.forum, page))
                return

            if data[-1]['updatedAt'] < target_date.isoformat():
                return

            params['before'] = data[-1]['id']
            yield data

    dcard.forums._get_paged_metas = get_paged_metas


if __name__ == '__main__':
    s = time.time()

    dcard = Dcard()
    name = 'bg'

    hack_forums_get_meta_function(dcard)

    bound = 60000
    metas = dcard.forums(name).get_metas(num=bound)
    print(name, len(metas))


    print('{:.05} sec'.format(time.time() - s))
