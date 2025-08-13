import sys
import signal
import threading
import os
import traceback
import json

from dotenv import load_dotenv
from atproto import Client
from flask import Flask, jsonify, request

from server import config, data_stream
from server.data_filter import operations_callback
from server.algos import algos
from server.algos.feed import handler as skeleton_handler


app = Flask(__name__)

stream_stop_event = threading.Event()
stream_thread = threading.Thread(
    target=data_stream.run, args=(config.SERVICE_DID, operations_callback, stream_stop_event,)
)
stream_thread.start()


def sigint_handler(*_):
    print('Stopping data stream...')
    stream_stop_event.set()
    sys.exit(0)


signal.signal(signal.SIGINT, sigint_handler)


@app.route('/')
def index():
    return 'ATProto Feed Generator powered by The AT Protocol SDK for Python (https://github.com/MarshalX/atproto).'


@app.route('/.well-known/did.json', methods=['GET'])
def did_json():
    if not config.SERVICE_DID.endswith(config.HOSTNAME):
        return '', 404

    return jsonify({
        '@context': ['https://www.w3.org/ns/did/v1'],
        'id': config.SERVICE_DID,
        'service': [
            {
                'id': '#bsky_fg',
                'type': 'BskyFeedGenerator',
                'serviceEndpoint': f'https://{config.HOSTNAME}'
            }
        ]
    })


@app.route('/xrpc/app.bsky.feed.describeFeedGenerator', methods=['GET'])
def describe_feed_generator():
    feeds = [{'uri': uri} for uri in algos.keys()]
    response = {
        'encoding': 'application/json',
        'body': {
            'did': config.SERVICE_DID,
            'feeds': feeds
        }
    }
    return jsonify(response)


@app.route('/xrpc/app.bsky.feed.getFeedSkeleton', methods=['GET'])
def get_feed_skeleton():
    feed = request.args.get('feed', default=None, type=str)
    algo = algos.get(feed)
    if not algo:
        return 'Unsupported algorithm', 400

    # Example of how to check auth if giving user-specific results:
    """
    from server.auth import AuthorizationError, validate_auth
    try:
        requester_did = validate_auth(request)
    except AuthorizationError:
        return 'Unauthorized', 401
    """

    try:
        cursor = request.args.get('cursor', default=None, type=str)
        limit = request.args.get('limit', default=20, type=int)
        body = algo(cursor, limit)
    except ValueError:
        return 'Malformed cursor', 400

    return jsonify(body)

@app.route('/detailed_feed.json', methods=['GET'])
def detailed_feed():
    # Make sure .env is loaded
    load_dotenv()
    try:
        # 1) Fetch skeleton
        sk = skeleton_handler(cursor=None, limit=20)
        uris = [item['post'] for item in sk['feed']]

        # 2) Hydrate via ATProto
        client = Client()
        client.login(os.environ['HANDLE'], os.environ['PASSWORD'])
        # supply the URIs inside a params dict
        resp = client.app.bsky.feed.get_posts({"uris": uris})


        # 3) Build JSON
        posts = []
        for p in resp.posts:
            posts.append({
                "uri": p.uri,
                "author": {
                    "did":         p.author.did,
                    "handle":      p.author.handle,
                    "display_name": p.author.display_name,
                },
                "record": {
                    "createdAt": p.record.created_at,
                    "text":      p.record.text,
                    "embed":     p.record.embed.model_dump() if p.record.embed else None
                }
            })

        return jsonify({
            "cursor": sk["cursor"],
            "posts":  posts
        })

    except Exception as e:
        # Print stack trace to terminal
        import traceback
        tb = traceback.format_exc()
        print(tb, file=sys.stderr)

        # Return JSON with error + traceback lines
        return jsonify({
            "error": str(e),
            "trace": tb.splitlines()
        }), 500

