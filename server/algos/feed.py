# server/feed.py

from datetime import datetime
from typing import Optional

from server import config
from server.database import Post

# Bluesky will look here for your feed URI
uri = config.FEED_URI
CURSOR_EOF = 'eof'


def handler(cursor: Optional[str], limit: int) -> dict:
    # 1) Base query: newest posts first
    base_q = Post.select() \
                 .order_by(Post.indexed_at.desc(), Post.cid.desc())

    # 2) Apply cursor if given
    if cursor and cursor != CURSOR_EOF:
        ts_str, cid = cursor.split("::", 1)
        ts = datetime.fromtimestamp(int(ts_str) / 1000)
        base_q = base_q.where(
            ((Post.indexed_at == ts) & (Post.cid < cid)) |
            (Post.indexed_at < ts)
        )

    # 3) Fetch exactly `limit` posts
    items = list(base_q.limit(limit))

    # 4) Build the feed payload
    feed = [{"post": p.uri} for p in items]

    # 5) Determine next cursor
    if len(items) == limit:
        last = items[-1]
        next_cursor = f"{int(last.indexed_at.timestamp() * 1000)}::{last.cid}"
    else:
        next_cursor = CURSOR_EOF

    return {
        "cursor": next_cursor,
        "feed":   feed,
    }
