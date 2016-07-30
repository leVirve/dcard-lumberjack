'''
Strategy:

    -* replace all metadata for all forums
    -* replace only recent metadata for all forums
    -* replace only need update metadata for all forums

'''

import time
import concurrent.futures

from pymongo import MongoClient

from dcard import Dcard, logger


client = MongoClient()
db = client['dcard-metas']

dcard = Dcard()


def brand_new_crawl(name):

    def store_metas(metas, forum):
        result = db[forum].insert_many(metas)
        logger.info('[database] #Forum {}: {} items'.format(forum, len(result)))

    s = time.time()
    dcard.forums(name).get_metas(
            num=Dcard.forums.infinite_page,
            callback=lambda metas, forum=name: store_metas(metas, forum)
        )
    logger.info('<{}> used {:.05} sec.'.format(name, time.time() - s))


def only_recent_crawl(name, boundary_date):

    def store_metas(metas, forum):
        bulk = db[forum].initialize_unordered_bulk_op()
        [
            bulk.find({'id': meta['id']}).upsert().replace_one(meta)
            for meta in metas
        ]
        result = bulk.execute()
        del result['upserted']
        logger.info('[database] #Forum {}: {}'.format(forum, result))

    s = time.time()
    metas = dcard.forums(name).get_metas(
            num=dcard.forums.infinite_page,
            timebound=boundary_date.isoformat(),
            callback=lambda metas, forum=name: store_metas(metas, forum)
        )
    logger.info('<{}> used {:.05} sec.'.format(name, time.time() - s))


def update_new_crawl(name, boundary_num=None, boundary_date=None):

    def store_metas(metas, forum):
        bulk = db[forum].initialize_unordered_bulk_op()
        [
            bulk.find({
                'id': meta['id'],
                'updatedAt': {'$gt': meta['updatedAt']}
            }).upsert().replace_one(meta)
            for meta in metas
        ]
        result = bulk.execute()
        del result['upserted']
        logger.info('[database] #Forum {}: {}'.format(forum, result))

    s = time.time()
    metas = dcard.forums(name).get_metas(
            num=dcard.forums.infinite_page if not boundary_num else boundary_num,
            timebound=boundary_date.isoformat() if boundary_date else '',
            callback=lambda metas, forum=name: store_metas(metas, forum)
        )
    logger.info('<{}> used {:.05} sec.'.format(name, time.time() - s))


def main():
    forums = dcard.forums.get(no_school=True)
    forums = [forum['alias'] for forum in forums]

    with concurrent.futures.ProcessPoolExecutor(max_workers=4) as executor:
        executor.map(brand_new_crawl, forums)


if __name__ == '__main__':
    s = time.time()
    main()
    print('Total Work: {:.05} sec'.format(time.time() - s))
