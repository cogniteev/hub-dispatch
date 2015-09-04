import copy

__version__ = (0, 0, 1)


class GraphBackend(object):
    """ Indirect graph whose nodes are either `hubs` or `nodes`. It is not
    possible to connect nodes together.
    """
    def __init__(self, hubs=None, links=None):
        """ Create a new graph

        :param set hubs: predefined hubs
        :param dict links: nodes connection: node -> set([node, ...])
        """
        self.__hubs = hubs or set()
        self.__links = links or {}

    def add_hub(self, hub):
        if hub in self.__hubs:
            raise Exception("Hub '{}' already exists".format(hub))
        self.__hubs.add(hub)
        assert hub not in self.__links, "node should not be in __links"
        self.__links[hub] = set()
        return self

    def remove_hub(self, hub):
        if hub not in self.__hubs:
            self._unknown_hub(hub)
        connected_hubs = []
        for node in self.__links.get(hub, []):
            if node not in self.__hubs:
                raise Exception("Can't remove hub with connected nodes")
            connected_hubs.append(node)
        for connected_hub in connected_hubs:
            self.unlink(hub, connected_hub)
        self.__hubs.remove(hub)
        return self

    def link(self, hub, node):
        if hub == node:
            raise Exception("Hub can't be linked to itself")
        if hub not in self.__hubs:
            self._unknown_hub(hub)
        nodes = self._links(hub)
        if node in nodes:
            error_message = "Hub '{}' is already connected to node '{}'"
            raise Exception(error_message.format(
                hub,
                node
            ))
        nodes.add(node)
        nodes = self._links(node).add(hub)
        return self

    def unlink(self, hub, node):
        if hub not in self.__hubs:
            self._unknown_hub(hub)
        nodes = self._links(hub)
        if node not in nodes:
            raise Exception("Hub '{}' is not connected to node '{}'".format(
                hub,
                node
            ))
        nodes.remove(node)
        nodes = self._links(node)
        nodes.remove(hub)
        if not any(nodes):
            self.__links.pop(node)
        return self

    def _links(self, node):
        return self.__links.setdefault(node, set())

    def links(self, node):
        if node not in self.__links:
            raise Exception("Unknown node '{}'".format(node))
        return copy.deepcopy(self._links(node))

    def hub_links(self, hub):
        if not self.is_hub(hub):
            self._unknown_hub(hub)
        return self.links(hub)

    def _unknown_hub(self, hub):
        raise Exception("Hub '{}' does not exist".format(hub))

    def hubs(self):
        return copy.copy(self.__hubs)

    def is_hub(self, hub):
        return hub in self.__hubs


def kruskall(g):
    hubs = g.hubs()
    all_hubs = g.hubs()
    cfcs = []
    while any(hubs):
        cfc = Backend()
        cfc_hubs = [hubs.pop()]
        while any(cfc_hubs):
            hub = cfc_hubs.pop()
            cfc.add_hub(hub)
            for node in cfc.links(hub):
                if node in all_hubs:
                    cfc.add_hub(node)
                    hubs.remove(hub)
                    cfc_hubs.add(hub)
                cfc.link(hub, node)
        cfcs.append(cfc)
    return cfcs


class HubDispatch(object):
    def __init__(self, graph, max_nodes_per_hub):
        self._graph = graph
        self._max_nodes_per_hub
        self._topology = {
            'nodes': {
            },
            'hubs': {
            },
        }

    def add_hub(self, hub):
        self._graph.link()
        # FIXME: redispatch

    def remove_hub(self, hub):
        for node in self._graph.links(hub):
            if not self._graph.is_hub(node):
                self.unlink(hub, node)
        self.__graph.remove_hub(hub)

    def link(self, hub, node):
        self.__graph.link(hub, node)
        self._reassign(hub, self.__graph.links(node))

    def unlink(self, hub, node):
        other_hubs = filter(
            lambda h: h != hub,
            self._graph.links(node)
        )
        self._reassign(node, other_hubs, [hub])
        self._graph.unlink(hub, node)

    def _reassign(self, node, candidates, black_list=[]):
        if not any(candidates):
            return
        current_hub = self._typology['nodes'].get(node)
        if current_hub is not None:
            if current_hub in candidates:
                return
            assert current_hub in black_list
            # find the least loaded hub among candidates
            candidate = reduce(
                self._least_loaded,
                candidates
            )
            if candidate >= self._max_nodes_per_hub:
                raise NotImplementedError()
            self._assign(node, candidate)

    def _assign(self, node, hub):
        current_hub = self._topology['nodes'].get(node)
        if current_hub is not None:
            nodes = self._topology['nodes'][current_hub]
            if nodes == 1:
                self._topology['nodes'].remove(current_hub)
            else:
                self._topology['nodes'][current_hub] = nodes - 1

    def _least_loaded(self, c1, c2):
        c1_load = self._topology['hubs'].get(c1, 0)
        c2_load = self._topology['hubs'].get(c2, 0)
        if c1_load <= c2_load:
            return c1
        else:
            return c2
