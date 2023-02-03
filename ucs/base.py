from abc import ABC, abstractmethod


class Driver(ABC):
    def __init__(
        self,
        key: str = None,
        secret: str = None,
        region: str = None,
        *args,
        **kwargs
    ):
        self.key = key
        self.secret = secret
        self.region = region

    @abstractmethod
    def upload_file(self, file, file_path: str) -> str:
        """
        Upload file to storage
        :return: uploaded file URL
        """
        raise NotImplementedError

    @abstractmethod
    def delete_file(self, file_path: str) -> None:
        """
        Delete file from storage
        :return:
        """
        raise NotImplementedError
