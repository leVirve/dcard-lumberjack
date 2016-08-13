import logging

from pymongo import MongoClient
from bson.objectid import ObjectId

logger = logging.getLogger(__name__)


class Datastore:

    client = MongoClient()
    meta_db = client['dcard-metas']
    post_db = client['dcard-posts']

    @classmethod
    def insert_metas(cls, metas, forum):
        result = cls.meta_db[forum].insert_many(metas)
        logger.info('[db] #Forum %s: %d', forum, len(result.inserted_ids))
        return result

    @classmethod
    def upsert_metas_if_newer(cls, metas, forum):
        if len(metas) == 0:
            return

        collect = cls.meta_db[forum]
        metas = [
            meta
            for meta in metas
            if not collect.find_one({
                'id': meta['id'],
                'updatedAt': {'$lte': meta['updatedAt']}
            })
        ]
        logger.info('<%s> collect %d metas need update.', forum, len(metas))

        bulk = cls.meta_db[forum].initialize_unordered_bulk_op()
        for meta in metas:
            meta['pending'] = True
            bulk.find({'id': meta['id']}).upsert().update_one({'$set': meta})
        result = bulk.execute()

        del result['upserted']

        logger.info('[db] <%s> update %d items: %s', forum, len(metas), result)
        return result

    @classmethod
    def find_pending_metas(cls, forum):
        return cls.meta_db[forum].find({'pending': True})

    @classmethod
    def finish_pending_meta(cls, forum, meta):
        cls.meta_db[forum].update_one(
            {'_id': ObjectId(meta['_id'])},
            {'$set': {'pending': False}})

    @classmethod
    def save(cls, post):
        cls.post_db.posts.update_one(
            {'id': post['id']},
            {'$set': post},
            upsert=True)
