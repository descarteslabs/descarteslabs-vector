class VectorException(Exception):
    """
    A base class for exceptions raised in the client.
    """

    pass


class RedirectException(VectorException):
    """
    An Exception class raised when the client experiences redirect errors
    when communicating to the server.
    """

    pass


class ClientException(VectorException):
    """
    An Exception class raised when the client experiences an internal error,
    possibly due to invalid user input.
    """

    pass


class ServerException(VectorException):
    """
    An Exception class raised when the client receives an error code(50x) from
    the server.
    """

    pass


class GenericException(VectorException):
    """
    An Exception class raised when the client encounters an error without
    sufficient context to narrow the possible causes.
    """

    pass


class NotFound(VectorException):
    """
    An Exception class raised when the client searches for a feature and cannot
    find it.
    """

    pass
