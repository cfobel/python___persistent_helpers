from ZODB import DB, MappingStorage
import transaction
from persistent_helpers.ordered_dict import PersistentOrderedDict


def load_database():
    storage = MappingStorage.MappingStorage()
    db = DB(storage)
    connection = db.open()
    root = connection.root()
    return storage, db, connection, root


def test_ordered_dict():
    from collections import OrderedDict

    labels = 'abcdefghijklmnopqrstuvwxyz'

    d = OrderedDict([(i, labels[i]) for i in range(len(labels))])

    storage, db, connection, root = load_database()

    root['test'] = PersistentOrderedDict(d.items())
    root['test'][0] = 'hello world'
    transaction.commit()

    connection = db.open()
    root = connection.root()

    assert(d.items()[1:] == root['test'].items()[1:])
    assert(root['test'][0] == 'hello world')

    root['test'] = d
    root['test'][0] = 'hello world'
    transaction.commit()

    connection = db.open()
    root = connection.root()

    assert(d.items()[1:] == root['test'].items()[1:])
    assert(root['test'][0] == 'hello world')


if __name__ == '__main__':
    test_ordered_dict()
