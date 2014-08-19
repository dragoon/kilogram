import re


class Parser(object):
    """
    Parser to remove all kinds of wiki markup tags from an object
    """

    def __init__(self):
        """
        Constructor
        """
        self.string = ''
        # all the following regex remove all tags that cannot be rendered
        # in text
        self.wiki_re = re.compile(r"""\[{2}(File|Category):[\s\S]+\]{2}|
                                        [\s\w#():-]+\||
                                        (\[{2}|\]{2})|
                                        \'{2,5}|
                                        (<s>|<!--)[\s\S]+(</s>|-->)|
                                        {{[\s\S]+}}|
                                        ^={1,6}|={1,6}$""", re.X)

    def __list(self, listmatch):
        return ' ' * (len(listmatch.group()) - 1) + '*'

    def __parse(self, string=''):
        """
        Parse a string to remove and replace all wiki markup tags
        """
        self.string = string
        self.string = self.wiki_re.sub('', self.string)
        # search for lists
        self.listmatch = re.search('^(\*+)', self.string)
        if self.listmatch:
            self.string = self.__list(self.listmatch) + re.sub('^(\*+)', '', self.string)
        return self.string

    def parse_string(self, string=''):
        """
        Parse a string object to de-wikified text
        """
        self.strings = string.splitlines(True)
        self.strings = [self.__parse(line) for line in self.strings]
        return ''.join(self.strings)