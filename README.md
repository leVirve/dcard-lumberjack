# dcard-lumberjack
**Lumberjack in Dcard !** :evergreen_tree:

This aims to dumping the resources on `Dcard`. 
*The project is under active development.*

## Requirements

- Python 3
- MongoDB
- Redis

## Usage

This project uses `MongoDB` as data storage layer, and `Redis` acts as `broker` and `result-backend` for `Celery`.
Make sure they are on for services first.

- Define all your own tasks in `spider.py`.

```bash
$ python spider.py
```

- Start celery workers in this way, `--pool=solo` is needed for `Windows`.

```bash
$ celery -A lumberjack worker [--pool=solo]
```

------

```bash
> dcard-lbj [forum_name] [strategies_name]

fetching metadata from <$forum>......
dumping the posts on <$forum>.....

Summary: 1500 posts in 60 sec.
```
