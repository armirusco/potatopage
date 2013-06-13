from .base import ObjectManager


class GCalObjectManager(ObjectManager):
    supports_cursors = True
    not_supported_params = ['maxResults', 'pageToken']

    def __init__(self, service, resource_type, params, query_property='list'):
        self.resource_type = resource_type
        # just like service.events()
        self.resource = getattr(service, resource_type)()
        # just like service.events().list
        self.query_func = getattr(self.resource, query_property)

        for key in self.not_supported_params:
            if key in params.keys():
                raise TypeError("%s is not supported by %s" % (key, self.__class__.__name__))

        self.params = params
        self.query_property = query_property

        self._starting_cursor = None

    def get_cache_key(self):
        cache_key_data = [self.resource_type, self.query_property]
        cache_key_data += [str(value) for value in self.params.values()]
        return "|".join(sorted(cache_key_data))
    cache_key = property(get_cache_key)

    def starting_cursor(self, cursor):
        self._starting_cursor = cursor

    @property
    def next_cursor(self):
        return self._latest_end_cursor

    def __getitem__(self, value):
        if isinstance(value, slice):
            start, max_items = value.start, value.stop

        if isinstance(value, int):
            max_items = value

        response = self._do_api_call(
            maxResults=max_items,
            pageToken=self._starting_cursor
        )

        obj_list = response.get('items')

        if obj_list is None:
            raise Exception('%s: No items found in response.' % self.__class__.__name__)

        self._latest_end_cursor = response.get('nextPageToken')

        return obj_list[value]


    def _do_api_call(self, **new_params):
        params = self.params.copy()
        params.update(**new_params)

        return self.query_func(**params).execute()

