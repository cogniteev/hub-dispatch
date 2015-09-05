import unittest

from hub_dispatch import GraphBackend


class TestGraph(unittest.TestCase):
    def test_empty_graph(self):
        """ Test properties of an empty graph """
        g = GraphBackend()
        self.assertEqual(g.hubs(), set())
        with self.assertRaises(Exception) as exc:
            g.unlink('foo', 'bar')
        self.assertEqual(exc.exception.message, "Hub 'foo' does not exist")
        with self.assertRaises(Exception) as exc:
            g.links('foo')
        self.assertEqual(exc.exception.message, "Unknown node 'foo'")
        with self.assertRaises(Exception) as exc:
            g.hub_links('foo')
        self.assertEqual(exc.exception.message, "Hub 'foo' does not exist")
        self.assertFalse(g.is_hub('foo'))
        # try to link an unknown hub to an unknown node
        with self.assertRaises(Exception) as exc:
            g.link('foo', 'bar')
        self.assertEqual(exc.exception.message, "Hub 'foo' does not exist")

    def test_add_hub(self):
        g = GraphBackend()
        g.add_hub('foo')
        self.assertTrue(g.is_hub('foo'))
        self.assertEqual(g.hubs(), set(['foo']))
        self.assertEqual(g.links('foo'), set())
        self.assertEqual(g.hub_links('foo'), set())
        with self.assertRaises(Exception) as exc:
            g.add_hub('foo')
        self.assertEqual(
            exc.exception.message,
            "Hub 'foo' already exists"
        )
        with self.assertRaises(Exception) as exc:
            g.unlink('foo', 'bar')
        self.assertEqual(
            exc.exception.message,
            "Hub 'foo' is not connected to node 'bar'"
        )

    def test_remove_hub_without_link(self):
        g = GraphBackend()
        with self.assertRaises(Exception) as exc:
            g.remove_hub('foo')
        self.assertEqual(
            exc.exception.message,
            "Hub 'foo' does not exist"
        )
        g.add_hub('foo')
        g.remove_hub('foo')
        # ensure hub is deleted
        self.assertFalse(g.is_hub('foo'))
        self.assertEqual(g.hubs(), set())
        # try to unlink a node from 'foo'
        with self.assertRaises(Exception) as exc:
            g.unlink('foo', 'bar')
        self.assertEqual(
            exc.exception.message,
            "Hub 'foo' does not exist"
        )

    def test_remove_hub_with_linked_hubs(self):
        g = GraphBackend()\
            .add_hub('h1').add_hub('h2').add_hub('h3')\
            .link('h1', 'h2').link('h1', 'h3').link('h2', 'h3')
        self.assertEqual(g.hub_links('h1'), set(['h2', 'h3']))
        self.assertEqual(g.hub_links('h2'), set(['h1', 'h3']))
        self.assertEqual(g.hub_links('h3'), set(['h1', 'h2']))
        # unpromote a hub to a simple node
        g.remove_hub('h1')
        self.assertFalse('h1' in g.hubs())
        self.assertEqual(g.links('h1'), set(['h2', 'h3']))
        self.assertEqual(g.links('h2'), set(['h1', 'h3']))
        self.assertEqual(g.links('h3'), set(['h1', 'h2']))
        with self.assertRaises(Exception) as exc:
            g.hub_links('h1')
        self.assertEqual(
            exc.exception.message,
            "Hub 'h1' does not exist"
        )
        self.assertEqual(g.hub_links('h2'), set(['h1', 'h3']))
        self.assertEqual(g.hub_links('h3'), set(['h1', 'h2']))
        # unpromote h2 to a simple node
        g.unlink('h2', 'h1')
        g.remove_hub('h2')
        self.assertFalse('h1' in g.hubs())
        self.assertFalse('h2' in g.hubs())
        self.assertTrue('h3' in g.hubs())
        self.assertEqual(g.links('h1'), set(['h3']))
        self.assertEqual(g.links('h2'), set(['h3']))
        self.assertEqual(g.links('h3'), set(['h1', 'h2']))

    def test_add_node_link(self):
        g = GraphBackend()
        g.add_hub('foo')
        with self.assertRaises(Exception) as exc:
            g.link('foo', 'foo')
        self.assertEqual(
            exc.exception.message,
            "Hub can't be linked to itself"
        )
        g.link('foo', 'bar')
        self.assertFalse(g.is_hub('bar'))
        self.assertEqual(g.hubs(), set(['foo']))
        self.assertEqual(g.links('foo'), set(['bar']))
        self.assertEqual(g.hub_links('foo'), set(['bar']))
        with self.assertRaises(Exception) as exc:
            g.hub_links('bar')
        self.assertEqual(
            exc.exception.message,
            "Hub 'bar' does not exist"
        )
        with self.assertRaises(Exception) as exc:
            g.link('foo', 'bar')
        self.assertEqual(
            exc.exception.message,
            "Hub 'foo' is already connected to node 'bar'"
        )

    def test_add_hub_link(self):
        g = GraphBackend().add_hub('foo', 'bar').link('foo', 'bar')
        self.assertTrue(g.is_hub('foo'))
        self.assertTrue(g.is_hub('bar'))
        self.assertEquals(g.links('foo'), set(['bar']))
        self.assertEquals(g.hub_links('foo'), set(['bar']))
        self.assertEquals(g.links('bar'), set(['foo']))
        self.assertEquals(g.hub_links('bar'), set(['foo']))
        with self.assertRaises(Exception) as exc:
            g.link('foo', 'bar')
        self.assertEquals(
            exc.exception.message,
            "Hub 'foo' is already connected to node 'bar'"
        )
        with self.assertRaises(Exception) as exc:
            g.link('bar', 'foo')
        self.assertEquals(
            exc.exception.message,
            "Hub 'bar' is already connected to node 'foo'"
        )

    def test_remove_node_link(self):
        g = GraphBackend()
        g.add_hub('foo')
        g.link('foo', 'bar')
        self.assertEqual(g.links('foo'), set(['bar']))
        self.assertEqual(g.links('bar'), set(['foo']))
        with self.assertRaises(Exception) as exc:
            g.remove_hub('foo')
        self.assertEqual(
            exc.exception.message,
            "Can't remove hub with connected nodes"
        )
        g.unlink('foo', 'bar')
        self.assertTrue(g.is_hub('foo'))
        self.assertFalse(g.is_hub('bar'))
        self.assertEqual(g.hubs(), set(['foo']))
        self.assertEqual(g.links('foo'), set())

    def test_remove_hub_link(self):
        g = GraphBackend().add_hub('foo', 'bar').link('foo', 'bar')
        g.remove_hub('bar')
        self.assertTrue(g.is_hub('foo'))
        self.assertFalse(g.is_hub('bar'))
        self.assertEqual(g.hubs(), set(['foo']))
        self.assertEqual(g.links('foo'), set(['bar']))

    def test_remove_node_link_with_several_hubs(self):
        g = GraphBackend()\
            .add_hub('h1').add_hub('h2')\
            .link('h1', 'node').link('h2', 'node')
        g.unlink('h1', 'node')
        self.assertEqual(g.hubs(), set(['h1', 'h2']))
        self.assertEqual(g.links('h1'), set())
        self.assertEqual(g.links('h2'), set(['node']))
        self.assertEqual(g.links('node'), set(['h2']))


if __name__ == '__main__':
    unittest.main()
