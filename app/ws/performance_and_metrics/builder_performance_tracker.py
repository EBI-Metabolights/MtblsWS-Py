import logging
import time

logger = logging.getLogger('wslog')


class BuilderPerformanceTracker:
    """
    Class to track each of the various builders. You push whatever variables you want to track as keyword arguments, and
    then use the methods in the tracker to update those variables. You can also create timers to track the duration of
    particular tasks.
    """

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            self.__setattr__(k, v)
        self._timers = {}

    def push(self, key, val):
        logger.info(getattr(self, key))
        logger.info(f'bpt: {key}:{val}')

        if all(isinstance(el, type(val)) for el in getattr(self, key)):
            list = getattr(self, key)
            logger.info(f'prev: {list}')
            list.append(val)
            logger.info(f'current: {list}')
            self.__setattr__(key, list)
        else:
            raise TypeError(f"attempt to push val {val} of type {type(val)} to list of type {type(getattr(self, key))}")

    def start_timer(self, timer_name: str):
        self._timers.update(
            {
                timer_name: {
                    'start_time': time.time(), 'end_time': None
                }
            }
        )

    def stop_timer(self, timer_name: str):
        self._timers[timer_name]['end_time'] = time.time()

    def get_duration(self, timer_name: str) -> float:
        return self._timers[timer_name]['end_time'] - self._timers[timer_name]['start_time']

    def report(self, key):
        attr = getattr(self, key)
        ktype = type(attr)
        if ktype is list:
            return f'{key} has the contents {str(attr)}'
        elif ktype is int:
            return f'Counter {key} hit {str(attr)}'
        elif ktype is str:
            return f'{key} has value {attr}'
        else:
            return f'{key} of type {ktype} returns this as a str(): {str(attr)}'

    def report_all_timers(self):
        return [f'\n {name} took {str(self.get_duration(name))} sec' for name, timer_dict in self._timers.items()]


