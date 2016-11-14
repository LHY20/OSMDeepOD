from src.base.tag import Tag


class Search:
    def __init__(self, parameters=None):
        if parameters is None: parameters = dict()
        self.word = parameters.get('word', 'crosswalk')
        self.tag = Tag(key=parameters.get('key', 'highway'), value=parameters.get('value', 'crossing'))
        self.zoom_level = parameters.get('zoom_level', 19)
        self.barrier = 0.99
        self.compare = parameters.get('compare', True)
        self.orthophoto = parameters.get('orthophoto', 'other')
        self.network = parameters.get('network', '')
        self.labels = parameters.get('labels', '')
        self.follow_streets = parameters.get('follow_street', True)
        self.bbox_size = float(parameters.get('bbox_size', 2000))
        self.timeout= parameters.get('timeout', 5400)

    def hit(self, prediction):
        return prediction[self.word] > self.barrier