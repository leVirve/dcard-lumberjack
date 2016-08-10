import logging

from pymongo import MongoClient

logger = logging.getLogger(__name__)


class DBStrategy:

    client = MongoClient(connect=False)
    db = client['dcard-metas']

    @staticmethod
    def insert_metas(metas, forum):
        collect = DBStrategy.db[forum]
        result = collect.insert_many(metas)
        logger.info('[db] #Forum %s: %d', forum, len(result.inserted_ids))
        return result

    @staticmethod
    def upsert_metas(metas, forum):
        collect = DBStrategy.db[forum]
        bulk = collect.initialize_unordered_bulk_op()
        for meta in metas:
            bulk.find({'id': meta['id']}).upsert().replace_one(meta)
        result = bulk.execute() if len(metas) else None
        if result:
            del result['upserted']
        logger.info('[db] #Forum %s: %s', forum, result)
        return result

    @staticmethod
    def upsert_metas_if_newer(metas, forum):
        collect = DBStrategy.db[forum]
        bulk = collect.initialize_unordered_bulk_op()
        for meta in metas:
            bulk.find({
                'id': meta['id'],
                'updatedAt': {'$gt': meta['updatedAt']}
            }).upsert().replace_one(meta)
        result = bulk.execute() if len(metas) else None
        if result:
            del result['upserted']
        logger.info('[db] #Forum %s: %s', forum, result)
        return result

    @staticmethod
    def find_older_metas(metas, forum):
        collect = DBStrategy.db[forum]
        result = [
            collect.find_one({
                'id': meta['id'],
                'updatedAt': {'$gt': meta['updatedAt']}
            })
            for meta in metas
        ]
        ret = [(r['_id'], meta) for r, meta in zip(result, metas) if r]
        logger.info('[db] #Forum %s: %d items need update', forum, len(ret))
        return ret
