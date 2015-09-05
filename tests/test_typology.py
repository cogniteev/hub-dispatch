import unittest

from hub_dispatch import HubDispatch


class TestTypology(unittest.TestCase):
    def test_empty_graph(self):
        h = HubDispatch()
        with self.assertRaises(Exception) as exc:
            h.remove_hub('foo')
        self.assertEqual(
            exc.exception.message,
            "Hub 'foo' does not exist"
        )
        with self.assertRaises(Exception) as exc:
            h.link('foo', 'bar')
        self.assertEqual(
            exc.exception.message,
            "Hub 'foo' does not exist"
        )
        with self.assertRaises(Exception) as exc:
            h.unlink('foo', 'bar')
        self.assertEqual(
            exc.exception.message,
            "Hub 'foo' does not exist"
        )

    def test_add_hub_linked_to_node(self):
        h = HubDispatch()
        h.add_hub('foo')
        self.assertEqual(h._changes.assignments, [])
        h.link('foo', 'bar')
        self.assertEqual(
            h._topology.nodes,
            {'bar': 'foo'},
        )
        self.assertEqual(
            h._topology.hubs,
            {'foo': 1},
        )
        self.assertEqual(h._changes.assignments, [('foo', 'bar')])
        h._changes._clear()

    def test_unlink_hub_with_lonely_node(self):
        h = HubDispatch().add_hub('foo').link('foo', 'bar')
        h._changes._clear()
        with self.assertRaises(Exception) as exc:
            h.unlink('foo', 'unknown-node')
        self.assertEqual(
            exc.exception.message,
            "Hub 'foo' is not connected to node 'unknown-node'"
        )
        h.unlink('foo', 'bar')
        self.assertEqual(h._topology.nodes, {})
        self.assertEqual(h._topology.hubs, {})
        self.assertEqual(h._changes.assignments, [])
        self.assertEqual(h._changes.unassignments, [('foo', 'bar')])

    def test_unlink_hub_with_lonely_nodes(self):
        h = HubDispatch().add_hub('foo').link('foo', 'n1').link('foo', 'n2')
        h._changes._clear()
        h.unlink('foo', 'n1')
        self.assertEqual(h._topology.nodes, {'n2': 'foo'})
        self.assertEqual(h._topology.hubs, {'foo': 1})
        self.assertEqual(h._changes.assignments, [])
        self.assertEqual(h._changes.unassignments, [('foo', 'n1')])

    def test_remove_hub_with_lonely_links(self):
        h = HubDispatch().add_hub('foo').link('foo', 'bar')
        h._changes._clear()
        h.remove_hub('foo')
        self.assertEqual(h._topology.nodes, {})
        self.assertEqual(h._topology.hubs, {})
        self.assertEqual(h._changes.assignments, [])
        self.assertEqual(h._changes.unassignments, [('foo', 'bar')])

    def test_remove_hub_with_shared_link(self):
        h = HubDispatch()\
            .add_hub('h1', 'h2')\
            .link('h1', 'node').link('h2', 'node')
        self.assertEqual(h._topology.nodes, {'node': 'h1'})
        self.assertEqual(h._topology.hubs, {'h1': 1})
        self.assertEqual(h._changes.assignments, [('h1', 'node')])
        self.assertEqual(h._changes.unassignments, [])
        h._changes._clear()
        # remove assigned hub
        h.unlink('h1', 'node')
        self.assertEqual(h._topology.nodes, {'node': 'h2'})
        self.assertEqual(h._topology.hubs, {'h2': 1})
        self.assertEqual(h._changes.assignments, [('h2', 'node')])
        self.assertEqual(h._changes.unassignments, [('h1', 'node')])

        # remove unassigned hub
        h = HubDispatch()\
            .add_hub('h1', 'h2')\
            .link('h1', 'node').link('h2', 'node')
        h._changes._clear()
        h.unlink('h2', 'node')
        self.assertEqual(h._topology.nodes, {'node': 'h1'})
        self.assertEqual(h._topology.hubs, {'h1': 1})

    def test_hub_addition_not_implemented(self):
        h = HubDispatch(max_nodes_per_hub=1)\
            .add_hub('h1', 'h2')\
            .link('h1', 'n1').link('h2', 'n1').link('h2', 'n2')
        h._changes._clear()
        with self.assertRaises(NotImplementedError):
            # 'h2's slots are full, the hub can't be assigned 'n1'
            h.unlink('h1', 'n1')
        # nothing must have been commited
        self.assertEqual(h._graph.hub_links('h1'), set(['n1']))
        self.assertEqual(h._graph.hub_links('h2'), set(['n1', 'n2']))
        self.assertEqual(h._topology.nodes, {'n1': 'h1', 'n2': 'h2'})
        self.assertEqual(h._topology.hubs, {'h1': 1, 'h2': 1})
        self.assertEqual(h._changes.assignments, [])
        self.assertEqual(h._changes.unassignments, [])

    def test_add_too_much_node(self):
        h = HubDispatch(max_nodes_per_hub=1).add_hub('h').link('h', 'n1')
        h._changes._clear()
        with self.assertRaises(NotImplementedError):
            h.link('h', 'n2')

    def test_assign_to_least_loaded(self):
        h = HubDispatch()\
            .add_hub('h1', 'h2', 'h3', 'h4')\
            .link('h1', 'node')\
            .link('h4', 'node', 'foo', 'plop', 'pika')\
            .link('h2', 'node', 'foo', 'bar')\
            .link('h3', 'node')
        h._changes._clear()
        h.unlink('h1', 'node')
        self.assertEqual(
            h._topology.nodes,
            {
                'foo': 'h4',
                'bar': 'h2',
                'node': 'h3',
                'plop': 'h4',
                'pika': 'h4'
            }
        )
        self.assertEqual(h._topology.hubs, {'h2': 1, 'h3': 1, 'h4': 3})
        self.assertEqual(h._changes.assignments, [('h3', 'node')])
        self.assertEqual(h._changes.unassignments, [('h1', 'node')])

    def test_reassignment_when_hub_have_multiple_assignees(self):
        h = HubDispatch()\
            .add_hub('h1', 'h2')\
            .link('h1', 'n1', 'n2')\
            .link('h2', 'n1')
        h._changes._clear()
        h.unlink('h1', 'n1')
        self.assertEqual(h._topology.nodes, {'n1': 'h2', 'n2': 'h1'})
        self.assertEqual(h._topology.hubs, {'h1': 1, 'h2': 1})
        self.assertEqual(h._changes.assignments, [('h2', 'n1')])
        self.assertEqual(h._changes.unassignments, [('h1', 'n1')])

    def test_least_loaded_func(self):
        h = HubDispatch()
        h._topology.hubs['foo'] = 0
        h._topology.hubs['bar'] = 42
        self.assertEqual('foo', h._least_loaded('foo', 'bar'))
        self.assertEqual('foo', h._least_loaded('bar', 'foo'))


if __name__ == '__main__':
    unittest.main()
