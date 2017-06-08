from KINCluster import Item

class myItem(Item):
    def dump(self):
        return {
            key: value
            for key, value in zip(self.keys, self.values)
        }

    def __hash__(self):
        return self.__e

    def __repr__(self):
        title_quote = " ".join(self.title_quote)
        content_quote = " ".join(self.content_quote)
        return " ".join([title_quote,
                         content_quote,
                         self.title,
                         self.content,
                         self.entities])

    def __str__(self):
        title_quote = " ".join(self.title_quote)
        content_quote = " ".join(self.content_quote)
        return " ".join([title_quote,
                         content_quote,
                         self.title,
                         self.content,
                         self.entities])
