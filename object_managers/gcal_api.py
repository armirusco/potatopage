from .base import ObjectManager


class GCalObjectManager(ObjectManager):
    supports_cursors = True

    def __init__(self, resource_type, params, query_property='list'):
        self.resource_type = resource_type
        self.params = params
        self.query_property = query_property

        self._starting_cursor = None

    def get_cache_key(self):
        cache_key_data = [self.resource_type, self.query_property]
        cache_key_data += self.params.values()
        return "|".join(cache_key_data)
    cache_key = property(get_cache_key)

    def starting_cursor(self, cursor):
        self._starting_cursor = cursor

    @property
    def next_cursor(self):
        return self._latest_end_cursor


