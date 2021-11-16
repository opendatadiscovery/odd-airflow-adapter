import inspect

from odd_models.utils import SqlParser as BaseSqlParser


class SqlParser(BaseSqlParser):
    def __init__(self, extractor, task, sql):
        self.extractor = extractor
        self._task = task
        super().__init__(sql)

    def get_inputs(self):
        return self.extractor.get_oddrn_list(self._task, self._inputs)

    def get_outputs(self):
        return self.extractor.get_oddrn_list(self._task, self._outputs)


def get_transformer_name(dag_id: str, task_id: str) -> str:
    return f"{dag_id}.{task_id}"


def get_transformer_run_name(run_id: str, task_id: str) -> str:
    return f"{run_id}.{task_id}"


def get_props(obj, exclude_list: list = None) -> dict:
    response = {}
    for name in dir(obj):
        if name not in exclude_list and not name.startswith('__') and not name.startswith('_'):
            try:
                value = getattr(obj, name)
                if not inspect.ismethod(value):
                    if isinstance(value, set):
                        response[name] = list(value)
                    elif not isinstance(value, (int, str, list, dict, bool)):
                        response[name] = str(value)
                    else:
                        response[name] = value
            except:
                continue
    return response
