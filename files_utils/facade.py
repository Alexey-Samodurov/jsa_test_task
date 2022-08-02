import typing as typ
import pandas as pd
from jsa_test_task.files_utils.filenames_collector import FileNamesCollector


def get_filenames(
        folder: str,
        pattern: str = None,
        is_full: bool = True,
) -> typ.List[str]:
    """
    Возвращает список файлов из заданной папки по заданному критерию.
    :param folder: папка
    :param pattern: паттерн регулярного выражения для отбора файлов
    :param is_full: указано True, если требуется вернуть полные имена файлов
    :return:
    """
    if is_full:
        return FileNamesCollector.collect_folder_full_filenames(folder=folder, pattern=pattern)
    else:
        return FileNamesCollector.collect_folder_filenames(folder=folder, pattern=pattern)