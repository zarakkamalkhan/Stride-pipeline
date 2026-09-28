import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pytest
from stride_pipeline.spark_session import get_spark


@pytest.fixture(scope="session")
def spark():
    s = get_spark("stride-pipeline-tests")
    s.sparkContext.setLogLevel("WARN")
    yield s
    s.stop()
