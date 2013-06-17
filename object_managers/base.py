class ObjectManager(object):
    """
    This is a base object manager making sure all sub-classes implement the
    right methods and properties.
    """
    supports_cursors = None

    def get_cache_key(self):
        """
        This should return a string that can be used as a unique cache key for
        a query considering its set properties like order, filters, etc.
        """
        raise NotImplemented()
    cache_key = property(get_cache_key)

    def starting_cursor(self, cursor):
        """
        This method should be used to set a cursor/token before actually doing
        the query by accessing a subset of a query's elements.
        """
        if self.supports_cursors:
            raise NotImplemented()

    @property
    def next_cursor(self):
        """
        Returns the cursor/token after a query has been made. The cursor needs
        to be cached on the object until this method is called.
        """
        if self.supports_cursors:
            raise NotImplemented()

    def __getitem__(self, value):
        """
        Doing the actual query to the given backend (DB, API, etc.), caching the
        next cursor so that it can be retrieved via self.next_cursor().
        """
        raise NotImplemented()

    def contains_more_objects(self, cursor):
        """
        Makes another query to check if there are any more objects available
        doing the same query with the passed in cursor (usually equal to
        self.next_cursor)

        N.B. This isn't used by the FilterablePaginator as it may cause many
             more backend requests just to figure this out.
        """
        raise NotImplemented()
