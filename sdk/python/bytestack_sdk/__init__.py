from .writer import LocalWriter
from .writer_s3 import S3Writer


def open_writer(path, controller=None):
    if str(path).startswith("s3://"):
        if controller is None:
            raise TypeError("s3:// writers require a controller address")
        return S3Writer.open(path, controller=controller)
    return LocalWriter.open(path, controller=controller)


__all__ = ["LocalWriter", "S3Writer", "open_writer"]
