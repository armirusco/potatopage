from .base import ObjectManager


class GCalObjectManager(ObjectManager):
    supports_cursors = True
    # Params that aren't supported because our ObjectManager sets them dynamically.
    not_supported_params = ['maxResults', 'pageToken']

    def __init__(self, service, resource_type, params, query_property='list'):
        """
        Saving the service and making the query function ready to be used with the API.
        """
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

        self._starting_token = None
        self._latest_end_token = None

    @property
    def cache_key(self):
        """
        Creating a unique cache key for this call with exactly the given params.

        Returns an unique string as cache key for this API call.
        """
        cache_key_data = [self.resource_type, self.query_property]
        cache_key_data += [str(value) for value in self.params.values()]
        return " ".join(sorted(cache_key_data)).replace(" ", "_")

    def starting_cursor(self, cursor):
        """
        Setting the start cursor. This needs to happen before we call the __getitem__()
        """
        self._starting_token = cursor
        self._latest_end_token = None

    @property
    def next_cursor(self):
        """
        Returns the end cursor of the last made call to the API.
        """
        return self._latest_end_token

    def __getitem__(self, value):
        """
        Preparing the actual API call and modifying the result so that we return a list of objects, no meta-data.

        Returns a list of objects, no meta data!
        """
        if isinstance(value, slice):
            start, max_items = value.start, value.stop

        if isinstance(value, int):
            max_items = value

        response = self._do_api_call(
            maxResults=max_items,
            pageToken=self._starting_token
        )

        obj_list = response.get('items')
        if obj_list is None:
            raise Exception('%s: No items found in response.' % self.__class__.__name__)

        self._latest_end_token = response.get('nextPageToken')
        return obj_list[value]


    def _do_api_call(self, **new_params):
        """
        Doing the actual API call after updating the params.
        """
        params = self.params.copy()
        params.update(**new_params)

        return self.query_func(**params).execute()

    def contains_more_objects(self, next_cursor):
        """
        Checking if the API gives us any more objects back for this set of params.
        """
        response = self._do_api_call(
            maxResults=1,
            pageToken=next_cursor
        )

        obj_list = response.get('items')
        return bool(obj_list)
