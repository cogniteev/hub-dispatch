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


# def kruskall(g):
#     hubs = g.hubs()
#     all_hubs = g.hubs()
#     cfcs = []
#     while any(hubs):
#         cfc = GraphBackend()
#         cfc_hubs = [hubs.pop()]
#         while any(cfc_hubs):
#             hub = cfc_hubs.pop()
#             cfc.add_hub(hub)
#             for node in cfc.links(hub):
#                 if node in all_hubs:
#                     cfc.add_hub(node)
#                     hubs.remove(hub)
#                     cfc_hubs.add(hub)
#                 cfc.link(hub, node)
#         cfcs.append(cfc)
#     return cfcs


class TypologyBackend(object):
    def __init__(self, nodes=None, hubs=None):
        """ Typology of node assignments among available hubs
        """
        self.nodes = nodes or {}
        self.hubs = hubs or {}


class TypologyChange(object):
    def __init__(self, **kwargs):
        self._clear()

    def _clear(self):
        self.assignments = []
        self.unassignments = []

    def assign(self, hub, node):
        self.assignments.append((hub, node))

    def unassign(self, hub, node):
        self.unassignments.append((hub, node))


class HubDispatch(object):
    def __init__(self, graph_cls=GraphBackend, graph_kwargs=None,
                 typology_cls=TypologyBackend, typology_kwargs=None,
                 typology_change_cls=TypologyChange,
                 typology_change_kwargs=None,
                 max_nodes_per_hub=100):
        self._graph = graph_cls(**(graph_kwargs or {}))
        self._topology = typology_cls(**(typology_kwargs or {}))
        self._changes = typology_change_cls(
            **(typology_change_kwargs or {})
        )
        self._max_nodes_per_hub = max_nodes_per_hub

    def add_hub(self, *hubs):
        for hub in hubs:
            self._graph.add_hub(hub)
        return self

    def remove_hub(self, hub):
        for node in self._graph.hub_links(hub):
            if not self._graph.is_hub(node):
                self.unlink(hub, node)
        self._graph.remove_hub(hub)

    def link(self, hub, *nodes):
        for node in nodes:
            self._graph.link(hub, node)
            if node not in self._topology.nodes:
                self._assign(node, hub)
        return self

    def unlink(self, hub, node):
        if node not in self._graph.hub_links(hub):
            raise Exception("Hub '{}' is not connected to node '{}'".format(
                hub,
                node
            ))
        other_hubs = filter(
            lambda h: h != hub,
            self._graph.links(node)
        )
        if self._topology.nodes[node] == hub:
            if any(other_hubs):
                self._reassign(node, other_hubs, [hub])
                assert self._topology.nodes[node] != hub
            else:
                assignee = self._topology.hubs[hub]
                assert assignee > 0, "should not have less than 1 assignee"
                if assignee == 1:
                    self._topology.hubs.pop(hub)
                else:
                    self._topology.hubs[hub] = assignee - 1
                self._topology.nodes.pop(node)
                self._changes.unassign(hub, node)
        self._graph.unlink(hub, node)
        return self

    def _reassign(self, node, candidates, black_list=[]):
        assert any(candidates), "there should be assignment candidates"
        # find the least loaded hub among candidates
        candidate = reduce(
            self._least_loaded,
            filter(
                lambda c: c not in black_list,
                candidates
            )
        )
        candidate_assignees = self._topology.hubs.get(candidate, 0)
        if candidate_assignees >= self._max_nodes_per_hub:
            raise NotImplementedError()  # FIXME
        self._assign(node, candidate)

    def _assign(self, node, hub):
        current_hub = self._topology.nodes.get(node)
        assert current_hub != hub, 'Node is already assigned to this hub'
        assignees = self._topology.hubs.get(hub, 0)
        if assignees >= self._max_nodes_per_hub:
            raise NotImplementedError()  # FIXME
        self._topology.nodes[node] = hub
        self._topology.hubs[hub] = assignees + 1
        if current_hub is not None:
            nodes = self._topology.hubs.get(current_hub, 0)
            if nodes == 1:
                self._topology.hubs.pop(current_hub)
            else:
                self._topology.hubs[current_hub] = nodes - 1
            self._changes.unassign(current_hub, node)
        self._changes.assign(hub, node)

    def _least_loaded(self, c1, c2):
        c1_load = self._topology.hubs.get(c1, 0)
        c2_load = self._topology.hubs.get(c2, 0)
        if c1_load <= c2_load:
            return c1
        else:
            return c2
