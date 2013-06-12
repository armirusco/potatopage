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
    def __init__(self, object_list, per_page, batch_size=1, *args, **kwargs):

        self._batch_size = batch_size

        if 'readahead' in kwargs.keys():
            raise TypeError('This paginator doesn\'t support the readahead argument.')

        super(FilterablePaginator, self).__init__(object_list, per_page, *args, **kwargs)

    def _get_final_page(self):
        final_obj = self._get_final_obj()
        return int(ceil(final_obj/float(self.per_page)))

    def _get_final_obj(self):
        key = "|".join([self.object_list.cache_key, "LAST_OBJ"])
        return cache.get(key)

    def _put_final_obj(self, obj):
        key = "|".join([self.object_list.cache_key, "LAST_OBJ"])
        cache.set(key, obj)

    def _get_known_page_count(self):
        last_known_obj = self._get_known_obj_count()
        return int(ceil(last_known_obj/float(self.per_page)))

    def _get_known_obj_count(self):
        key = "|".join([self.object_list.cache_key, "KNOWN_OBJ_COUNT"])
        return cache.get(key)

    def _put_known_obj_count(self, count):
        key = "|".join([self.object_list.cache_key, "KNOWN_OBJ_COUNT"])
        return cache.put(key, count)

    def _put_cursor(self, zero_based_obj, cursor):
        if not self.object_list.supports_cursors or cursor is None:
            return

        logging.info("Storing cursor for page: %s" % (zero_based_obj))
        key = "|".join([self.object_list.cache_key, str(zero_based_obj)])
        cache.set(key, cursor)

    def _get_cursor(self, zero_based_obj):
        logging.info("Getting cursor for obj: %s" % (zero_based_obj))
        key = "|".join([self.object_list.cache_key, str(zero_based_obj)])
        result = cache.get(key)
        if result is None:
            raise CursorNotFound("No cursor available for %s" % zero_based_obj)
        return result

    def has_cursor_for_page(self, page):
        try:
            self._get_cursor(page-1)
            return True
        except CursorNotFound:
            return False

    def validate_number(self, number):
        "Validates the given 1-based page number."
        try:
            number = int(number)
        except (TypeError, ValueError):
            raise PageNotAnInteger('That page number is not an integer')
        if number < 1:
            raise EmptyPage('That page number is less than 1')

        return number

    def _objs_with_cursors(self):
        key = "|".join([self.object_list.cache_key, 'CURSORED_OBJECTS'])
        result = cache.get(key)
        if result is None:
            return []
        return result

    def _find_nearest_obj_with_cursor(self, current_object):
        #Find the next object down that should be storing a cursor
        cursored_objects = self._objs_with_cursors()
        if not cursored_objects:
            return 0

        return max([c_obj for c_obj in cursored_objects if c_obj <=
                    current_object])

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

    def page(self, number):
        number = self.validate_number(number)
        start_index = number * self.per_page

        start_cursor, offset = self._get_cursor_and_offset(start_index)
        next_cursor = None

        filtered_objects = []
        while len(filtered_objects) < self._batch_size:
            if next_cursor:
                start_cursor = next_cursor
                next_cursor = None

            if start_cursor:
                self.object_list.starting_cursor(start_cursor)
                results = self.object_list[:self._batch_size]
            else:
                bottom = start_index - offset
                top = bottom + self._batch_size
                results = self.object_list[bottom:top]

            filtered_results = filter(self.filter_func, results)
            end_index = start_index + len(filtered_results)

            filtered_objects.append(filtered_results)

            if len(results) < self._batch_size:
                break

            if self.object_list.supports_cursors:
                #Store the cursor at the start of the NEXT batch
                next_cursor = self.object_list.next_cursor
                self._put_cursor(end_index, next_cursor)

        batch_result_count = len(filtered_objects)

        actual_results = filtered_objects[offset:offset + self.per_page]

        if not actual_results:
            if number == 1 and self.allow_empty_first_page:
                pass
            else:
                raise EmptyPage('That page contains no results')

        known_obj_count = int((start_index - offset) + batch_result_count)

        if known_obj_count >= self._get_known_obj_count():
            if batch_result_count < self._batch_size:
                # We reached the end of the object list.
                self._put_final_obj(known_obj_count)

            self._put_known_page_count(known_obj_count)
        return UnifiedPage(actual_results, number, self)

    def _get_count(self):
        raise NotImplemented("Not available in %s" % self.__class__.__name__)

    def _get_num_pages(self):
        raise NotImplemented("Not available in %s" % self.__class__.__name__)


class UnifiedPage(Page):
    def __init__(self, object_list, number, paginator):
        super(UnifiedPage, self).__init__(object_list, number, paginator)

    def __repr__(self):
        """ Overwrite paginator's repr, so no Exception gets thrown
            because the number of pages is unknown.
        """
        return '<Page %s>' % (self.number)

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

    def available_pages(self, limit_to_batch_size=True):
        """
            Returns a list of sorted integers that represent the
            pages that should be displayed in the paginator. In relation to the
            current page. For example, if this page is page 3, and batch_size is 5
            we get the following:

            [ 1, 2, *3*, 4, 5, 6, 7, 8 ]

            If we then choose page 7, we get this:

            [ 2, 3, 4, 5, 6, *7*, 8, 9, 10, 11, 12 ]

            If limit_to_batch_size is False, then you always get all known pages
            this will generally be the same for the upper count, but the results
            will always start at 1.
        """
        min_page = (self.number - self.paginator._batch_size) if limit_to_batch_size else 1
        if min_page < 1:
            min_page = 1

        max_page = min(self.number + self.paginator._batch_size, self.paginator._get_known_page_count())
        return list(xrange(min_page, max_page + 1))

    def __repr__(self):
        return '<UnifiedPage %s>' % self.number


