

class UnexpectedStatusException(Exception):
    """
    Raised when an HTTP response returns an unexpected status code.

    Attributes:
        status (int): The HTTP status code received.
        expected (tuple[int, ...] | None): Expected status codes.
        url (str | None): Request URL.
        message (str): Human-readable error message.
    """

    def __init__(
        self,
        status: int,
        task_id: int,
        expected: tuple[int, ...] | None = None,
        url: str | None = None,
        message: str | None = None,
        worker_id: int | None = None,
    ):
        self.status = status
        self.expected = expected
        self.url = url

        expected_str = f", expected={expected}" if expected else ""
        url_str = f", url={url}" if url else ""
        self.message = f"Unexpected HTTP status: {task_id=}{worker_id=},\t{status}{expected_str}{url_str}\n{message=}"

        super().__init__(self.message)
