class ObjectManager(object):
    supports_cursors = None

    def get_cache_key(self):
        raise NotImplemented()
    cache_key = property(get_cache_key)

    def starting_cursor(self, cursor):
        if self.supports_cursors:
            raise NotImplemented()

    @property
    def next_cursor(self):
        if self.supports_cursors:
            raise NotImplemented()

    def __getitem__(self, value):
        raise NotImplemented()

    def contains_more_objects(self):
        raise NotImplemented()
