from collections import OrderedDict

from path import path
import durus.file_storage
import durus.client_storage
import durus.connection
import durus.btree
import durus.persistent_dict
import durus.persistent_list
import durus.persistent_set
import transaction

from .durus_types import PersistentOrderedDict


class BaseTransactionalStorage(object):
    default_storage_host = None
    default_storage_port = None

    def __init__(self, host=None, port=None):
        if host is None:
            host = self.default_storage_host
        if port is None:
            port = self.default_storage_port
        self.host = host
        self.port = port
        self.reset_db()

    def abort(self):
        raise NotImplementedError, ('%s is an abstract base class' %
                                    self.__class__)

    def commit(self):
        raise NotImplementedError, ('%s is an abstract base class' %
                                    self.__class__)

    def reset_db(self):
        raise NotImplementedError, ('%s is an abstract base class' %
                                    self.__class__)

    def __len__(self):
        raise NotImplementedError, ('%s is an abstract base class' %
                                    self.__class__)

    def __getitem__(self, db_path):
        nodes = db_path.split('/')
        assert(nodes[0] == '')
        node_names = nodes[1:]
        node = self.root
        for i in range(len(node_names)):
            if node_names[i] not in node:
                node[node_names[i]] = PersistentOrderedDict()
            node = node[node_names[i]]
        return node

    def __setitem__(self, db_path, value):
        db_path = path(db_path)
        assert(db_path.parent)
        node = self[str(db_path.parent)]
        node[str(db_path.name)] = value
        return node


class DurusStorage(BaseTransactionalStorage):
    default_storage_host = '127.0.0.1'
    default_storage_port = 2972

    def __init__(self, cache_size=100, **kwargs):
        self.cache_size = cache_size
        super(DurusStorage, self).__init__(**kwargs)

    def abort(self):
        self.connection.abort()

    def commit(self):
        self.connection.commit()

    def reset_db(self):
        if self.port:
            self.connection = durus.connection.Connection(
                    durus.client_storage.ClientStorage(self.host, self.port),
                    cache_size=self.cache_size
            )
        else:
            self.connection = durus.connection.Connection(
                    durus.file_storage.FileStorage(self.host),
                    cache_size=self.cache_size
            )
        self.root = self.connection.get_root()

    def __setitem__(self, db_path, value):
        if isinstance(value, OrderedDict):
            value = PersistentOrderedDict(value.items())
        elif isinstance(value, dict):
            value = durus.persistent_dict.PersistentDict(value.items())
        elif isinstance(value, list):
            value = durus.persistent_list.PersistentList(value)
        elif isinstance(value, set):
            value = durus.persistent_set.PersistentSet(value)
        super(DurusStorage, self).__setitem__(db_path, value)

    def __len__(self):
        return len(self.root.keys())


class ZodbStorage(BaseTransactionalStorage):
    default_storage_host = '127.0.0.1'
    default_storage_port = 2972

    def abort(self):
        transaction.abort()

    def commit(self):
        transaction.commit()

    def reset_db(self):
        from ZODB import DB
        from ZEO import ClientStorage

        if self.port:
            self._storage = ClientStorage.ClientStorage((self.host, self.port))
        else:
            self._storage = ClientStorage.ClientStorage(self.host)
        self._db = DB(self._storage)
        self.connection = self._db.open()
        self.root = self.connection.root()


