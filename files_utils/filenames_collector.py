from typing import List
import os
import re
from loguru import logger


class FileNamesCollector:

    @staticmethod
    def collect_folder_filenames(
        folder,
        pattern=None,
    ) -> List[str]:
        filenames = [filename for filename in os.listdir(folder) if '~' not in filename]
        if pattern:
            filenames = [
                filename for filename in filenames if re.search(pattern, filename)
            ]
        return filenames

    @staticmethod
    def collect_folder_full_filenames(
            folder: str,
            pattern=None,
    ) -> List[str]:
        filenames = FileNamesCollector.collect_folder_filenames(folder, pattern)
        full_filenames = [os.path.join(folder, filename) for filename in filenames]
        return full_filenames

    @staticmethod
    def collect_generated_filenames(generated_filenames: List[str]) -> List[str]:
        collected_filenames = []
        for filename in generated_filenames:
            if os.path.exists(filename):
                collected_filenames.append(filename)
        if len(collected_filenames) != len(generated_filenames):
            FileNamesCollector.write_not_existing_filenames_to_logger(collected_filenames, generated_filenames)
            raise FileNotFoundError
        return collected_filenames

    @staticmethod
    def write_not_existing_filenames_to_logger(collected_filenames: List[str], generated_filenames: List[str]):
        logger.debug('Количество сгенерированных файлов отличается от количества собранных.')
        logger.debug(f'Искал файлов: {len(generated_filenames)}')
        logger.debug(f'Нашел файлов: {len(collected_filenames)}')
        for generated_filename in generated_filenames:
            if generated_filename not in collected_filenames:
                logger.debug(f'нет такого файла: {generated_filename}')
