import atexit
import datetime
import faulthandler
import os

# import platform
# import subprocess
import tempfile
import unittest

# import netCDF4
# import numpy as np

faulthandler.enable()  # to debug seg faults and timeouts

import cfdm

warnings = False

# Set up temporary files
n_tmpfiles = 1
tmpfiles = [
    tempfile.mkstemp("_test_read_write.nc", dir=os.getcwd())[1]
    for i in range(n_tmpfiles)
]
[tmpfile1] = tmpfiles


def _remove_tmpfiles():
    """Remove temporary files created during tests."""
    for f in tmpfiles:
        try:
            os.remove(f)
        except OSError:
            pass


atexit.register(_remove_tmpfiles)


class UGRIDTest(unittest.TestCase):
    """Test UGRID field constructs."""

    filename = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "ugrid_1.nc"
    )

    def setUp(self):
        """Preparations called immediately before each test method."""
        # Disable log messages to silence expected warnings
        cfdm.LOG_LEVEL("DISABLE")
        # Note: to enable all messages for given methods, lines or
        # calls (those without a 'verbose' option to do the same)
        # e.g. to debug them, wrap them (for methods, start-to-end
        # internally) as follows: cfdm.LOG_LEVEL('DEBUG')
        #
        # < ... test code ... >
        # cfdm.log_level('DISABLE')

    def test_read_UGRID(self):
        """TODOUGRID."""
        f = cfdm.read(self.filename)
        self.assertEqual(len(f), 3)
        for g in f:
            self.assertEqual(len(g.domain_topologies()), 1)
            self.assertEqual(len(g.auxiliary_coordinates()), 2)
            self.assertEqual(len(g.dimension_coordinates()), 1)

        # Check that all fields have the same mesh id
        mesh_ids = set(g.get_mesh_id() for g in f)
        self.assertEqual(len(mesh_ids), 1)


if __name__ == "__main__":
    print("Run date:", datetime.datetime.now())
    cfdm.environment()
    print("")
    unittest.main(verbosity=2)
