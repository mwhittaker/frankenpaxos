from . import cluster
from . import host
import unittest


class ClusterTest(unittest.TestCase):
    def test_good_configuration(self):
        json = """
        {
            "1": {
                "leaders": ["l0", "l1"],
                "acceptors": ["a0", "a1", "a2"]
            },
            "2": {
                "leaders": ["l0", "l1", "l2"],
                "acceptors": ["a0", "a1", "a2", "a3", "a4"]
            }
        }
        """
        c = cluster.Cluster.from_json_string(json, lambda a: host.FakeHost(a))
        f1 = c.f(1)
        self.assertEqual(
            f1['leaders'],
            [host.FakeHost('l0'), host.FakeHost('l1')])
        self.assertEqual(
            f1['acceptors'],
            [host.FakeHost('a0'),
             host.FakeHost('a1'),
             host.FakeHost('a2')])
        f2 = c.f(2)
        self.assertEqual(
            f2['leaders'],
            [host.FakeHost('l0'),
             host.FakeHost('l1'),
             host.FakeHost('l2')])
        self.assertEqual(f2['acceptors'], [
            host.FakeHost('a0'),
            host.FakeHost('a1'),
            host.FakeHost('a2'),
            host.FakeHost('a3'),
            host.FakeHost('a4')
        ])

    def _test_bad_f(self):
        json = """ { "a": {} } """
        c = cluster.Cluster.from_json_string(json, lambda a: host.FakeHost(a))

    def test_bad_f(self):
        self.assertRaises(ValueError, self._test_bad_f)

    def _test_bad_addresses(self):
        json = """
        {
            "1": {
                "leaders": 42
            }
        }
        """
        c = cluster.Cluster.from_json_string(json, lambda a: host.FakeHost(a))

    def test_bad_addresses(self):
        self.assertRaises(ValueError, self._test_bad_addresses)

    def _test_bad_address(self):
        json = """
        {
            "1": {
                "leaders": [42]
            }
        }
        """
        c = cluster.Cluster.from_json_string(json, lambda a: host.FakeHost(a))

    def test_bad_address(self):
        self.assertRaises(ValueError, self._test_bad_address)


if __name__ == '__main__':
    unittest.main()
