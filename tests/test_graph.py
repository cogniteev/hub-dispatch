import unittest

from hub_dispatch import Backend


class TestGraph(unittest.TestCase):
    def test_empty_graph(self):
        """ Test properties of an empty graph
        """
        g = Backend()
        self.assertEqual(g.hubs(), set())
        with self.assertRaises(Exception) as exc:
            g.unlink('foo', 'bar')
        self.assertEqual(
            exc.exception.message,
            "Hub 'foo' does not exist"
        )
        with self.assertRaises(Exception) as exc:
            g.links('foo')
        self.assertEqual(
            exc.exception.message,
            "Unknown node 'foo'"
        )
        self.assertFalse(g.is_hub('foo'))
        # try to link an unknown hub to an unknown node
        with self.assertRaises(Exception) as exc:
            g.link('foo', 'bar')
        self.assertEqual(
            exc.exception.message,
            "Hub 'foo' does not exist"
        )

    def test_add_hub(self):
        g = Backend()
        g.add_hub('foo')
        self.assertTrue(g.is_hub('foo'))
        self.assertEqual(g.hubs(), set(['foo']))
        self.assertEqual(g.links('foo'), set())
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
        g = Backend()
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
        g = Backend()\
            .add_hub('h1').add_hub('h2').add_hub('h3')\
            .link('h1', 'h2').link('h1', 'h3').link('h2', 'h3')
        self.assertEqual(g.links('h1'), set(['h2', 'h3']))
        self.assertEqual(g.links('h2'), set(['h1', 'h3']))
        self.assertEqual(g.links('h3'), set(['h1', 'h2']))
        g.remove_hub('h1')
        self.assertFalse('h1' in g.hubs())
        self.assertEqual(g.links('h2'), set(['h3']))
        self.assertEqual(g.links('h3'), set(['h2']))

    def test_add_link(self):
        g = Backend()
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
        with self.assertRaises(Exception) as exc:
            g.link('foo', 'bar')
        self.assertEqual(
            exc.exception.message,
            "Hub 'foo' is already connected to node 'bar'"
        )

    def test_remove_node_link(self):
        g = Backend()
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

    def test_remove_node_link_with_several_hubs(self):
        g = Backend()\
            .add_hub('h1').add_hub('h2')\
            .link('h1', 'node').link('h2', 'node')
        g.unlink('h1', 'node')
        self.assertEqual(g.hubs(), set(['h1', 'h2']))
        self.assertEqual(g.links('h1'), set())
        self.assertEqual(g.links('h2'), set(['node']))
        self.assertEqual(g.links('node'), set(['h2']))


if __name__ == '__main__':
    unittest.main()
