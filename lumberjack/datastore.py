import logging

from pymongo import MongoClient
from bson.objectid import ObjectId

logger = logging.getLogger(__name__)


class Datastore:

    client = MongoClient(connect=False)
    meta_db = client['dcard-metas']
    post_db = client['dcard-posts']

    @classmethod
    def insert_metas(cls, metas, forum):
        result = cls.meta_db[forum].insert_many(metas)
        logger.info('[db] #Forum %s: %d', forum, len(result.inserted_ids))
        return result

    @classmethod
    def upsert_metas(cls, metas, forum):
        bulk = cls.meta_db[forum].initialize_unordered_bulk_op()
        for meta in metas:
            bulk.find({'id': meta['id']}).upsert().replace_one(meta)
        result = bulk.execute() if len(metas) else None
        if result:
            del result['upserted']
        logger.info('[db] #Forum %s: %s', forum, result)
        return result

    @classmethod
    def upsert_metas_if_newer(cls, metas, forum):
        bulk = cls.meta_db[forum].initialize_unordered_bulk_op()
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
        cls.post_db.posts.insert(post)
