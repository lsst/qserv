#! /usr/bin/env python
from __future__ import print_function
from lsst.qserv.master import protocol
import unittest


class ProtocolSanityTest(unittest.TestCase):
    """Tests sanity in wire protocol utils..
    """

    def setUp(self):
        global _options
        pass

    def tearDown(self):
        pass

    def testBuild(self):
        factory = protocol.TaskMsgFactory(123, "elephant")
        for i in range(1, 3):
            factory.newChunk("r_%d" % i, i)
            for i in range(1, 3):
                factory.fillFragment("SELECT * from blah;",
                                     range(i*10, (i+1)*10))
        msg = factory.getBytes()
        print("sizeof message is", len(msg))
        self.assertEqual(1, 1)


def main():
    global _options
    unittest.main()


if __name__ == "__main__":
    main()
