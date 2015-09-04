import copy

__version__ = (0, 0, 1)


class Backend(object):
    """ Indirect graph whose nodes are either `hubs` or `nodes`. It is not
    possible to connect nodes together.
    """
    def __init__(self):
        self.__hubs = set()
        self.__links = {}

    def add_hub(self, hub):
        if hub in self.__hubs:
            raise Exception("Hub '{}' already exists".format(hub))
        self.__hubs.add(hub)

    def remove_hub(self, hub):
        if hub not in self.__hubs:
            self._unknown_hub(hub)
        connected_hubs = []
        for node in self.__links[hub]:
            if node not in self.__hubs:
                raise Exception("Cannot remove hub with connected nodes")
            connected_hubs.append(node)
        for connected_hub in connected_hubs:
            self.unlink(hub, connected_hub)
        self.__hubs.remove(hub)

    def link(self, hub, node):
        if hub == node:
            raise Exception("Node cannot be connected to itself")
        if hub not in self.__hubs:
            self._unknown_hub(hub)
        nodes = self._links(hub)
        if node in nodes:
            raise Exception("Hub '{}' is already connect to node '{}'".format(
                hub,
                node
            ))
        nodes.add(node)
        nodes = self._link(node).add(hub)

    def unlink(self, hub, node):
        if hub not in self.__hubs:
            self._unknown_hub()
        nodes = self._links(hub)
        if node not in nodes:
            raise Exception("Hub '{}' is not connected to node '{}'".format(
                hub,
                node
            ))
        nodes.remove(node)
        if not any(nodes):
            self.__links.remove(node)
        # update links of connected node
        nodes = self.__links(node)
        nodes.remove(hub)
        if not any(nodes):
            self.__links.remove(node)

    def _links(self, node):
        return self.__links.setdefault(node, set())

    def links(self, node):
        return copy.deepcopy(self._links(node))

    def _unknown_hub(self, hub):
        raise Exception("Hub '{}' doe not exist".format(hub))

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
