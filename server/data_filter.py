import datetime
from collections import defaultdict

from atproto import models

from server import config
from server.logger import logger
from server.database import db, Post


def is_archive_post(record: 'models.AppBskyFeedPost.Record') -> bool:
    archived_threshold = datetime.timedelta(days=1)
    created_at = datetime.datetime.fromisoformat(record.created_at)
    now = datetime.datetime.now(datetime.UTC)
    return now - created_at > archived_threshold


def should_ignore_post(created_post: dict) -> bool:
    record = created_post['record']
    uri    = created_post['uri']

    if config.IGNORE_ARCHIVED_POSTS and is_archive_post(record):
        logger.debug(f'Ignoring archived post: {uri}')
        return True

    if config.IGNORE_REPLY_POSTS and record.reply:
        logger.debug(f'Ignoring reply post: {uri}')
        return True

    return False


def operations_callback(ops: defaultdict) -> None:
    posts_to_create = []

    for created_post in ops[models.ids.AppBskyFeedPost]['created']:
        record = created_post['record']
        uri    = created_post['uri']

        # lowercase text for matching
        text_lower = (record.text or "").lower()

        # skip archived/reply posts
        if should_ignore_post(created_post):
            continue

        # only keep posts whose text matches your keywords
        if not any(kw in text_lower for kw in config.VIDEO_KEYWORDS):
            continue

        # build your DB dict
        reply_root   = record.reply.root.uri   if record.reply else None
        reply_parent = record.reply.parent.uri if record.reply else None

        posts_to_create.append({
            'uri':          uri,
            'cid':          created_post['cid'],
            'reply_root':   reply_root,
            'reply_parent': reply_parent,
        })

        logger.debug(f'✅ KEEPING post {uri}')

    # handle deletions
    posts_to_delete = ops[models.ids.AppBskyFeedPost]['deleted']
    if posts_to_delete:
        uris = [p['uri'] for p in posts_to_delete]
        Post.delete().where(Post.uri.in_(uris)).execute()
        logger.debug(f'Deleted from feed: {len(uris)}')

    # insert new
    if posts_to_create:
        with db.atomic():
            for pd in posts_to_create:
                Post.create(**pd)
        logger.debug(f'Added to feed: {len(posts_to_create)}')

