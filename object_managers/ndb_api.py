from google.appengine.datastore.datastore_query import Cursor

from .base import ObjectManager


class NdbModelObjectManager(ObjectManager):
    """
    An object manager for ndb models.

    TODO: Proper testing, so far it has only be used to write a few tests for
          the FilterablePaginator.
    """
    supports_cursors = True

    def __init__(self, query):
        self.query = query
        self._starting_cursor = None
        self._contians_more_entities = None
        self._latest_end_cursor = None

    def get_cache_key(self):
        return " ".join([
            str(self.query._Query__kind),
            str(self.query._Query__ancestor),
            str(self.query._Query__filters),
            str(self.query._Query__orders),
            str(self.query._Query__app),
            str(self.query._Query__namespace)
        ]).replace(" ", "_")
    cache_key = property(get_cache_key)

    def starting_cursor(self, cursor):
        self._starting_cursor = Cursor(urlsafe=cursor)
        # we need to set those to None, otherwise a second query with the same
        # paginator would fail as the cursors are already set.
        self._latest_end_cursor = None
        self._contians_more_entities = None


    @property
    def next_cursor(self):
        return self._latest_end_cursor

    def __getitem__(self, value):
        if isinstance(value, slice):
            start, max_items = value.start, value.stop

        if isinstance(value, int):
            max_items = value

        entities, cursor, more = self.query.fetch_page(
            max_items,
            start_cursor=self._starting_cursor
        )

        self._starting_cursor = None
        self._latest_end_cursor = cursor.urlsafe()
        self._contians_more_entities = more

        return entities[value]

    def contains_more_objects(self, next_cursor):
        if self._contians_more_entities is not None:
            return self._contians_more_entities

        entities, cursor, more = self.query.fetch_page(
            1,
            start_cursor=next_cursor
        )

        entity_list = list(entities)
        return bool(entity_list)