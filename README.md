# dcard-lumberjack
**Lumberjack in Dcard !** :evergreen_tree:

This will be a project aims to dumping the resources on `Dcard`.

```bash
> dcard-lbj [forum_name] [strategies_name]

fetching metadata from <$forum>......
dumping the posts on <$forum>.....

Summary: 1500 posts in 60 sec.
```

*The project is under active development. Comming soon*

Use `MongoDB` as data storage layer, and `Redis` acts as `broker` and `result-backend` for `Celery`.

## Requirements

- Python 3
- MongoDB
- Redis

## Usage

- Run `MongoDB` and `redis-server`

```
$ python supervisor.py
```

- Run task in `spider.py`

```
$ python spider.py
```

- Run celery worker

```
$ celery -A lumberjack worker [--pool=solo]
```
