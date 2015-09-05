import unittest

from hub_dispatch import HubDispatch


class TestHubAllocation(unittest.TestCase):
    def test_hub_addition_not_implemented(self):
        h = HubDispatch(max_nodes_per_hub=2)\
            .add_hub('h1', 'h2')\
            .link('h1', 'n1').link('h2', 'n1').link('h2', 'n2')
        h._changes._clear()
        with self.assertRaises(NotImplementedError):
            # 'h2's slots are full, the hub can't be assigned 'n1'
            h.unlink('h1', 'n1')
        # nothing must have been commited
        self.assertEqual(h._graph.hub_links('h1'), set(['n1']))
        self.assertEqual(h._graph.hub_links('h2'), set(['n1', 'n2']))
        self.assertEqual(h._topology.nodes, {
            'h1': 'h1', 'h2': 'h2',
            'n1': 'h1', 'n2': 'h2'
        })
        self.assertEqual(h._topology.hubs, {'h1': 2, 'h2': 2})
        self.assertEqual(h._changes.assignments, [])
        self.assertEqual(h._changes.unassignments, [])

    def test_add_too_much_node(self):
        h = HubDispatch(max_nodes_per_hub=2).add_hub('h').link('h', 'n1')
        h._changes._clear()
        with self.assertRaises(NotImplementedError):
            h.link('h', 'n2')


if __name__ == '__main__':
    unittest.main()
