# vim:fileencoding=utf-8


class CgException(Exception):
    pass




class CgClientException(CgException):
    pass


class CgConnectException(CgException):
    pass


class CgDisconnectException(CgException):
    pass


class CgPubException(CgException):
    pass


class CgSubException(CgException):
    pass


class CgModelException(CgException):
    pass


class CgConfigException(CgException):
    pass

