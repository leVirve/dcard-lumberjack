import time
import concurrent.futures

from pymongo import MongoClient

from dcard import Dcard, logger


client = MongoClient()
db = client['dcard-metas']


def store_metas(metas, forum):
    bulk = db[forum].initialize_unordered_bulk_op()
    [bulk.find({'id': meta['id']}).upsert().replace_one(meta) for meta in metas]
    result = bulk.execute()

    result['upserted'] = len(result['upserted'])
    logger.info('[database] #Forum {}: {}'.format(forum, result))


def collect_metas(name):
    bound = 99999999
    s = time.time()
    Dcard.forums(name).get_metas(
            num=bound,
            callback=lambda metas, forum=name: store_metas(metas, forum)
        )
    logger.info('<{}> used {:.05} sec.'.format(name, time.time() - s))


def main():
    forums = Dcard.forums.get(no_school=True)
    forums = [forum['alias'] for forum in forums]

    with concurrent.futures.ProcessPoolExecutor(max_workers=4) as executor:
        executor.map(collect_metas, forums)


if __name__ == '__main__':
    s = time.time()
    main()
    print('Total Work: {:.05} sec'.format(time.time() - s))
