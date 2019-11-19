import logging
from typing import Union

from py4j.protocol import Py4JJavaError

from openeo.error_summary import ErrorSummary
from openeogeotrellis.GeotrellisCatalogImageCollection import GeotrellisCatalogImageCollection
from openeogeotrellis.GeotrellisImageCollection import GeotrellisTimeSeriesImageCollection
from openeogeotrellis._version import __version__
from openeogeotrellis.configparams import ConfigParams
from openeogeotrellis.errors import SpatialBoundsMissingException
from openeogeotrellis.utils import kerberos

logger = logging.getLogger("openeo")
logger.setLevel(logging.INFO)
log_formatter = logging.Formatter("%(asctime)s [%(levelname)s - THREAD: %(threadName)s - %(name)s] : %(message)s")

log_stream_handler = logging.StreamHandler()
log_stream_handler.setFormatter(log_formatter)
logger.addHandler( log_stream_handler )


def get_backend_version() -> str:
    return __version__


def create_process_visitor():
    from .geotrellis_tile_processgraph_visitor import GeotrellisTileProcessGraphVisitor
    return GeotrellisTileProcessGraphVisitor()



def summarize_exception(error: Exception) -> Union[ErrorSummary, Exception]:
    if isinstance(error, Py4JJavaError):
        java_exception = error.java_exception

        while java_exception.getCause() is not None and java_exception != java_exception.getCause():
            java_exception = java_exception.getCause()

        java_exception_class_name = java_exception.getClass().getName()
        java_exception_message = java_exception.getMessage()

        no_data_found = (java_exception_class_name == 'java.lang.AssertionError'
                         and "Cannot stitch empty collection" in java_exception_message)

        is_client_error = java_exception_class_name == 'java.lang.IllegalArgumentException' or no_data_found
        summary = "Cannot construct an image because the given boundaries resulted in an empty image collection" if no_data_found else java_exception_message

        return ErrorSummary(error, is_client_error, summary)

    if isinstance(error, SpatialBoundsMissingException):
        return ErrorSummary(error, is_client_error=True, summary="spatial bounds missing")

    return error


# Late import to avoid circular dependency issues.
# TODO avoid this. Also see https://github.com/Open-EO/openeo-geopyspark-driver/issues/12
from openeogeotrellis.backend import get_openeo_backend_implementation

