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
    def __init__(self, object_list, per_page, batch_size=None, filter_func=None, *args, **kwargs):

        if batch_size is None:
            batch_size = per_page

        self._batch_size = batch_size
        self.filter_func = filter_func

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

    def validate_number(self, number):
        "Validates the given 1-based page number."
        try:
            number = int(number)
        except (TypeError, ValueError):
            raise PageNotAnInteger('That page number is not an integer')
        if number < 1:
            raise EmptyPage('That page number is less than 1')

        return number

    def _add_obj_with_cursor_to_cached_list(self, obj_pos):
        obj_list = self._get_objs_with_cursors()
        logging.info('Cursored objects before: %s' % obj_list)
        if not obj_pos in obj_list:
            obj_list.append(obj_pos)

        logging.info('Cursored objects after: %s' % obj_list)
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

    def _find_nearest_obj_with_cursor(self, current_object):
        #Find the next object down that should be storing a cursor
        cursored_objects = self._get_objs_with_cursors()
        obj_lower_than_current = None

        if cursored_objects:
            obj_lower_than_current = [c_obj for c_obj in cursored_objects
                                      if c_obj <= current_object]

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

    def page(self, number):
        number = self.validate_number(number) - 1
        page_start_index = number * self.per_page
        logging.info('Page (%s) start index: %s' % (number, page_start_index))

        start_cursor, offset = self._get_cursor_and_offset(page_start_index)
        next_cursor = None
        logging.info('Start cursor: %s' % start_cursor)
        logging.info('Start offset: %s' % offset)

        filtered_objects = []
        while len(filtered_objects) < self._batch_size or len(filtered_objects) - offset < self.per_page:
            if next_cursor:
                start_cursor = next_cursor
                next_cursor = None

            if start_cursor:
                self.object_list.starting_cursor(start_cursor)
                results = self.object_list[:self._batch_size]
            else:
                bottom = page_start_index - offset
                top = bottom + self._batch_size
                results = self.object_list[bottom:top]

            # The filter function should be required, but let's leave it for now optional.
            if self.filter_func:
                filtered_results = filter(self.filter_func, results)
            else:
                filtered_results = results

            logging.info('filtered_results: %s' % len(filtered_results))
            logging.info('results: %s' % len(results))

            filtered_objects += filtered_results

            next_page_start_index = (page_start_index - offset) + len(filtered_objects)

            if len(results) < self._batch_size:
                break

            if self.object_list.supports_cursors:
                #Store the cursor at the start of the NEXT batch
                next_cursor = self.object_list.next_cursor
                self._add_obj_with_cursor_to_cached_list(next_page_start_index)
                self._put_cursor(next_page_start_index, next_cursor)

        batch_result_count = len(filtered_objects)

        actual_results = filtered_objects[offset:offset + self.per_page]

        if not actual_results:
            if number == 1 and self.allow_empty_first_page:
                pass
            else:
                raise EmptyPage('That page contains no results')

        known_obj_count = int((page_start_index - offset) + batch_result_count)

        if known_obj_count >= self._get_known_obj_count():
            if batch_result_count < self._batch_size:
                # We reached the end of the object list.
                self._put_final_obj(known_obj_count)

            self._put_known_obj_count(known_obj_count)
        return FilteredPage(actual_results, number, self)

    def _get_count(self):
        raise NotImplemented("Not available in %s" % self.__class__.__name__)

    def _get_num_pages(self):
        raise NotImplemented("Not available in %s" % self.__class__.__name__)


class FilteredPage(Page):
    def __init__(self, object_list, number, paginator):
        super(FilteredPage, self).__init__(object_list, number, paginator)

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

    def available_pages(self):
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
        num_pages_per_batch = ceil(self.paginator._batch_size/float(self.paginator.per_page))
        min_page = 1
        max_page = min(self.number + num_pages_per_batch, self.paginator._get_known_page_count())
        return list(xrange(min_page, max_page + 1))

    def __repr__(self):
        return '<FilteredPage %s>' % self.number


