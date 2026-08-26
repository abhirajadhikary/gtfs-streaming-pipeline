import logging
import requests
from google.transit import gtfs_realtime_pb2

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GTFSFetcher:
    def __init__(self, feed_url: str, timeout: int = 10):
        self.feed_url = feed_url
        self.timeout = timeout

    def fetch_feed(self) -> gtfs_realtime_pb2.FeedMessage:
        """
        Fetches the GTFS Realtime feed from the specified URL.
        """
        try:
            response = requests.get(self.feed_url, timeout=self.timeout)
            response.raise_for_status()
            feed = gtfs_realtime_pb2.FeedMessage()
            feed.ParseFromString(response.content)
            logger.info(f"Successfully fetched feed from {self.feed_url} with {len(feed.entity)} entities.")
            return feed
        except requests.RequestException as e:
            logger.error(f"Error fetching GTFS feed from {self.feed_url}: {e}")
            return None
        except Exception as e:
            logger.error(f"Error parsing GTFS feed from {self.feed_url}: {e}")
            return None