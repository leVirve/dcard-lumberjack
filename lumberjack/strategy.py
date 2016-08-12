import logging

from pymongo import MongoClient
from bson.objectid import ObjectId

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
            meta['pending'] = True
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
    def find_pending_metas(forum):
        collect = DBStrategy.db[forum]
        return collect.find({'pending': True})

    @staticmethod
    def change_pending_meta(forum, meta):
        collect = DBStrategy.db[forum]
        collect.update_one(
            {'_id': ObjectId(meta['_id'])},
            {'$set': {'pending': False}})


class Datastore:

    client = MongoClient(connect=False)
    db = client['dcard-posts']

    def save(self, post):
        self.db.posts.insert(post)
