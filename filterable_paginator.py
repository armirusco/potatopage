import logging
from math import ceil

from django.core.cache import cache
from django.core.paginator import (
    Paginator,
    EmptyPage,
    PageNotAnInteger,
    Page
)


class CursorNotFound(Exception):
    pass


class FilterablePaginator(Paginator):
    """
    A paginator that allows you to filter queried results before adding them to
    the list of objects.

    N.B. Please take care using this, filtering means just the query for one
         page may result in multiple queries to your object manager. I.e. it may
         result in multiple queries to the DB, API or which ever manager you
         may use.
    """
    def __init__(self, object_list, per_page, batch_size=None, filter_func=None, *args, **kwargs):
        """
        Params:
        object_list: a potatopage.object_managers.base.ObjectManager sub-class
        batch_size: this indicates how many objects should be queried when
            retrieving objects via the object_manager and how many filtered
            objects should be gathered before returning the page.
        filter_func: a function that can be passed into python's built-in
            filter() funciton. It should take the object as an argument and
            return True or False depending on if that element should be included
            or not.
        """
        if batch_size is None:
            batch_size = per_page

        self._batch_size = batch_size
        self._filter_func = filter_func

        if 'readahead' in kwargs.keys():
            raise TypeError('This paginator doesn\'t support the readahead argument.')

        super(FilterablePaginator, self).__init__(object_list, per_page, *args, **kwargs)

    def _get_final_page(self):
        final_obj = self._get_final_obj()
        if final_obj is None:
            return None
        return int(ceil(final_obj/float(self.per_page)))

    def _get_final_obj(self):
        key = "|".join([self.object_list.cache_key, "LAST_OBJ"])
        return cache.get(key)

    def _put_final_obj(self, obj):
        key = "|".join([self.object_list.cache_key, "LAST_OBJ"])
        cache.set(key, obj)

    def _get_known_page_count(self):
        last_known_obj = self._get_known_obj_count()
        if last_known_obj is None:
            return None
        return int(ceil(last_known_obj/float(self.per_page)))

    def _get_known_obj_count(self):
        key = "|".join([self.object_list.cache_key, "KNOWN_OBJ_COUNT"])
        return cache.get(key)

    def _put_known_obj_count(self, count):
        key = "|".join([self.object_list.cache_key, "KNOWN_OBJ_COUNT"])
        return cache.set(key, count)

    def _put_cursor(self, zero_based_obj, cursor):
        if not self.object_list.supports_cursors or cursor is None:
            return

        logging.info("Storing cursor for obj: %s" % (zero_based_obj))
        key = "|".join([self.object_list.cache_key, str(zero_based_obj)])
        cache.set(key, cursor)

    def _get_cursor(self, zero_based_obj):
        logging.info("Getting cursor for obj: %s" % (zero_based_obj))
        key = "|".join([self.object_list.cache_key, str(zero_based_obj)])
        result = cache.get(key)
        if result is None:
            raise CursorNotFound("No cursor available for %s" % zero_based_obj)
        return result

    def _add_obj_with_cursor_to_cached_list(self, obj_pos):
        """
        Small wrapper around the getting and setting of the updated list of
        cursored objects. I.e. this list contains the zero-based index of each
        object for which we have a cursor stored.
        """
        obj_list = self._get_objs_with_cursors()
        if not obj_pos in obj_list:
            obj_list.append(obj_pos)

        self._put_objs_with_cursors(obj_list)

    def _put_objs_with_cursors(self, value):
        key = "|".join([self.object_list.cache_key, 'CURSORED_OBJECTS'])
        cache.set(key, value)

    def _get_objs_with_cursors(self):
        key = "|".join([self.object_list.cache_key, 'CURSORED_OBJECTS'])
        result = cache.get(key)
        if result is None:
            return []
        return result

    def _find_nearest_obj_with_cursor(self, current_obj_index):
        #Find the next object down that should be storing a cursor
        cursored_objects = self._get_objs_with_cursors()
        obj_lower_than_current = None

        if cursored_objects:
            obj_lower_than_current = [c_obj for c_obj in cursored_objects
                                      if c_obj <= current_obj_index]

        if not obj_lower_than_current:
            return 0

        return max(obj_lower_than_current)

    def _get_cursor_and_offset(self, start_index):
        """ Returns a cursor and offset for the page. page is zero-based! """

        offset = 0
        cursor = None
        obj_with_cursor = self._find_nearest_obj_with_cursor(start_index)

        if self.object_list.supports_cursors:
            if obj_with_cursor > 0:
                try:
                    cursor = self._get_cursor(obj_with_cursor)
                    logging.info("Using existing cursor from memcache")
                except CursorNotFound:
                    logging.info("Couldn't find a cursor")
                    #No cursor found, so we just return the offset old-skool-style.
                    cursor = None

        offset = start_index - obj_with_cursor

        return cursor, offset

    def validate_number(self, number):
        "Validates the given 1-based page number."
        try:
            number = int(number)
        except (TypeError, ValueError):
            raise PageNotAnInteger('That page number is not an integer')
        if number < 1:
            raise EmptyPage('That page number is less than 1')

        return number

    def page(self, number):
        number = self.validate_number(number)
        zero_based_number = number - 1
        page_start_index = zero_based_number * self.per_page

        start_cursor, offset = self._get_cursor_and_offset(page_start_index)
        next_cursor = None
        start_bottom = page_start_index - offset

        filtered_objects = []
        # Keep looping querying the object manager until:
        # 1. You have a batch-length of filtered objects and enough objects to
        #    fill a whole page.
        # 2. The results of the query are less than a batch-length already
        #    before filtering. I.e. you're at the end of the list.
        while len(filtered_objects) < self._batch_size or len(filtered_objects) - offset < self.per_page:
            if next_cursor:
                start_cursor = next_cursor
                next_cursor = None

            if start_cursor:
                self.object_list.starting_cursor(start_cursor)
                results = self.object_list[:self._batch_size]
            else:
                bottom = start_bottom
                top = bottom + self._batch_size
                results = self.object_list[bottom:top]

            # The filter function should be required, but let's leave it for now optional.
            if self._filter_func:
                filtered_results = filter(self._filter_func, results)
            else:
                filtered_results = results

            filtered_objects += filtered_results

            next_page_start_index = (page_start_index - offset) + len(filtered_objects)

            if len(results) < self._batch_size:
                # This means we reached the end of the list.
                next_cursor = None
                break

            if self.object_list.supports_cursors:
                #Store the cursor at the start of the NEXT batch
                next_cursor = self.object_list.next_cursor
                self._add_obj_with_cursor_to_cached_list(next_page_start_index)
                self._put_cursor(next_page_start_index, next_cursor)
            else:
                start_bottom = top

        # What's the actual length of filtered objects (will need that for calculations)
        batch_result_count = len(filtered_objects)
        actual_results = filtered_objects[offset:offset + self.per_page]

        if not actual_results:
            if number == 1 and self.allow_empty_first_page:
                pass
            else:
                raise EmptyPage('That page contains no results')

        known_obj_count = int((page_start_index - offset) + batch_result_count)

        # If we have a new highest known object index, we want to cache it.
        if known_obj_count >= self._get_known_obj_count():
            if batch_result_count < self._batch_size:
                # We reached the end of the object list.
                self._put_final_obj(known_obj_count)
            elif next_cursor:
                # Assuming there is another batch because of the cursor
                known_obj_count += 1

            self._put_known_obj_count(known_obj_count)

        return FilteredPage(actual_results, number, self)

    def _get_count(self):
        raise NotImplemented("Not available in %s" % self.__class__.__name__)

    def _get_num_pages(self):
        raise NotImplemented("Not available in %s" % self.__class__.__name__)


class FilteredPage(Page):
    """
    A page containing objects that have been filtered by the FilterablePaginator.
    """

    def __repr__(self):
        """ Overwrite paginator's repr, so no Exception gets thrown
            because the number of pages is unknown.
        """
        return '<FilteredPage %s>' % (self.number)

    def has_next(self):
        return self.number < self.paginator._get_known_page_count()

    def start_index(self):
        """ Override to prevent returning 0 """
        if self.number == 0:
            return 1
        return (self.paginator.per_page * (self.number - 1)) + 1

    def end_index(self):
        """ Override to prevent a call to _get_count """
        return self.number * self.paginator.per_page

    def final_page_visible(self):
        return self.paginator._get_final_page() in self.available_pages()

    def available_pages(self):
        """
            Returns a list of sorted integers that represent the
            pages that should be displayed in the paginator. In relation to the
            current page. For example, if this page is page 3, and batch_size is 5
            we get the following:

            [ 1, 2, *3*, 4, 5, 6, 7, 8 ]
        """
        num_pages_per_batch = int(ceil(self.paginator._batch_size/float(self.paginator.per_page)))
        min_page = 1
        max_page = min(self.number + num_pages_per_batch, self.paginator._get_known_page_count())
        return list(xrange(min_page, max_page + 1))
