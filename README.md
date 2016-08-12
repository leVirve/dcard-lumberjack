# dcard-lumberjack
**Lumberjack in Dcard !** :evergreen_tree:

This will be a project aims to dumping the resources on `Dcard`.

## Requirements

- Python 3
- MongoDB
- Redis

## Usage

- Run `MongoDB` and `redis-server`
- Run task in `spider.py`

```bash
$ python supervisor.py

$ python spider.py
```


- (Optional) You can run more celery workers in this way, `--pool=solo` is needed for `Windows`.

```bash
$ celery -A lumberjack worker [--pool=solo]
```

- TBD

```bash
> dcard-lbj [forum_name] [strategies_name]

fetching metadata from <$forum>......
dumping the posts on <$forum>.....

Summary: 1500 posts in 60 sec.
```

*The project is under active development. Comming soon*

Use `MongoDB` as data storage layer, and `Redis` acts as `broker` and `result-backend` for `Celery`.
