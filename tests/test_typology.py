import unittest

from hub_dispatch import HubDispatch


class TestTopology(unittest.TestCase):
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
        self.assertEqual(h._changes.assignments, [('foo', 'foo')])
        h.link('foo', 'bar')
        self.assertEqual(
            h._topology.nodes,
            {'bar': 'foo', 'foo': 'foo'},
        )
        self.assertEqual(
            h._topology.hubs,
            {'foo': 2},
        )
        self.assertEqual(h._changes.assignments, [
            ('foo', 'foo'),
            ('foo', 'bar')
        ])
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
        self.assertEqual(h._topology.nodes, {'foo': 'foo'})
        self.assertEqual(h._topology.hubs, {'foo': 1})
        self.assertEqual(h._changes.assignments, [])
        self.assertEqual(h._changes.unassignments, [('foo', 'bar')])

    def test_unlink_hub_with_lonely_nodes(self):
        h = HubDispatch().add_hub('foo').link('foo', 'n1').link('foo', 'n2')
        h._changes._clear()
        h.unlink('foo', 'n1')
        self.assertEqual(h._topology.nodes, {'n2': 'foo', 'foo': 'foo'})
        self.assertEqual(h._topology.hubs, {'foo': 2})
        self.assertEqual(h._changes.assignments, [])
        self.assertEqual(h._changes.unassignments, [('foo', 'n1')])

    def test_remove_hub_with_lonely_links(self):
        h = HubDispatch().add_hub('foo').link('foo', 'bar')
        h._changes._clear()
        h.remove_hub('foo')
        self.assertEqual(h._topology.nodes, {})
        self.assertEqual(h._topology.hubs, {})
        self.assertEqual(h._changes.assignments, [])
        self.assertEqual(h._changes.unassignments, [
            ('foo', 'foo'),
            ('foo', 'bar')
        ])

    def test_remove_hub_following_hub(self):
        h = HubDispatch().add_hub('foo', 'bar').link('foo', 'bar')
        self.assertEqual(h._topology.nodes, {'bar': 'bar', 'foo': 'foo'})
        self.assertEqual(h._topology.hubs, {'foo': 1, 'bar': 1})
        self.assertEqual(h._changes.assignments, [
            ('foo', 'foo'), ('bar', 'bar')
        ])
        self.assertEqual(h._changes.unassignments, [])
        h._changes._clear()
        h.remove_hub('foo')
        self.assertEqual(h._topology.nodes, {'bar': 'bar'})
        self.assertEqual(h._topology.hubs, {'bar': 1})
        self.assertEqual(h._changes.assignments, [])
        self.assertEqual(h._changes.unassignments, [('foo', 'foo')])

    def test_hub_follows_followee(self):
        h = HubDispatch().add_hub('foo', 'bar')\
            .link('foo', 'bar').link('bar', 'foo')
        self.assertEqual(h._topology.nodes, {'foo': 'foo', 'bar': 'bar'})
        self.assertEqual(h._topology.hubs, {'foo': 1, 'bar': 1})
        self.assertEqual(h._changes.assignments, [
            ('foo', 'foo'),
            ('bar', 'bar'),
        ])
        self.assertEqual(h._changes.unassignments, [])

    def test_remove_hub_with_shared_link(self):
        h = HubDispatch()\
            .add_hub('h1', 'h2')\
            .link('h1', 'node').link('h2', 'node')
        self.assertEqual(h._topology.nodes,
                         {'node': 'h1', 'h1': 'h1', 'h2': 'h2'})
        self.assertEqual(h._topology.hubs, {'h1': 2, 'h2': 1})
        self.assertEqual(h._changes.assignments,
                         [('h1', 'h1'), ('h2', 'h2'), ('h1', 'node')])
        self.assertEqual(h._changes.unassignments, [])
        h._changes._clear()
        # remove assigned hub
        h.unlink('h1', 'node')
        self.assertEqual(h._topology.nodes,
                         {'h1': 'h1', 'h2': 'h2', 'node': 'h2'})
        self.assertEqual(h._topology.hubs, {'h2': 2, 'h1': 1})
        self.assertEqual(h._changes.assignments, [('h2', 'node')])
        self.assertEqual(h._changes.unassignments, [('h1', 'node')])

    def test_remove_unassigned_hub(self):
        h = HubDispatch()\
            .add_hub('h1', 'h2')\
            .link('h1', 'node').link('h2', 'node')
        h._changes._clear()
        h.unlink('h2', 'node')
        self.assertEqual(h._topology.nodes, {
            'node': 'h1',
            'h1': 'h1',
            'h2': 'h2',
        })
        self.assertEqual(h._topology.hubs, {'h1': 2, 'h2': 1})

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
            {'h1': 'h1', 'h2': 'h2', 'h3': 'h3', 'h4': 'h4', 'foo': 'h4',
             'bar': 'h2', 'node': 'h3', 'plop': 'h4', 'pika': 'h4'}
        )
        self.assertEqual(h._topology.hubs,
                         {'h2': 2, 'h3': 2, 'h4': 4, 'h1': 1})
        self.assertEqual(h._changes.assignments, [('h3', 'node')])
        self.assertEqual(h._changes.unassignments, [('h1', 'node')])

    def test_reassign_node_on_hub_removal(self):
        h = HubDispatch().add_hub('A', 'C').link('A', 'B').link('C', 'B')
        self.assertEqual(h._topology.nodes, {
            'A': 'A', 'B': 'A', 'C': 'C'
        })
        h._changes._clear()
        h.remove_hub('A')
        self.assertEqual(h._topology.nodes, {
            'B': 'C', 'C': 'C'
        })
        self.assertEqual(h._changes.unassignments, [('A', 'A'), ('A', 'B')])
        self.assertEqual(h._changes.assignments, [('C', 'B')])

    def test_promote_hub_a_followed_node(self):
        h = HubDispatch().add_hub('A').link('A', 'B')
        h._changes._clear()
        h.add_hub('B')
        self.assertEqual(h._topology.nodes, {
            'A': 'A', 'B': 'A'
        })
        self.assertEqual(h._topology.hubs, {'A': 2})

    def test_reassignment_when_hub_have_multiple_assignees(self):
        h = HubDispatch()\
            .add_hub('h1', 'h2')\
            .link('h1', 'n1', 'n2')\
            .link('h2', 'n1')
        h._changes._clear()
        h.unlink('h1', 'n1')
        self.assertEqual(h._topology.nodes, {
            'h1': 'h1', 'h2': 'h2',
            'n1': 'h2', 'n2': 'h1',
        })
        self.assertEqual(h._topology.hubs, {'h1': 2, 'h2': 2})
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
