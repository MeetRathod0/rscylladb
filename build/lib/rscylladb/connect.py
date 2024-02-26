from .logger import get_logger
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import os

logger = get_logger(os.path.basename(__file__)+"_get_connection")


def get_connection(hosts: list, port: int, keyspace: str, username: str = 'cassandra', password: str = 'cassandra'):
    try:
        auth = PlainTextAuthProvider(username=username, password=password)
        cluster = Cluster(contact_points=hosts, port=port, auth_provider=auth)
        logger.info(f"Connection establised...{str(hosts)}:{port}")
        session = cluster.connect(keyspace)
        logger.info(f"Keyspace: {keyspace} is conncted.")
        return cluster, session
    except Exception as e:
        logger.error("\n{}\nERROR".format('-'*30), exc_info=e)
        exit(1)


__all__ = ["get_connection"]
