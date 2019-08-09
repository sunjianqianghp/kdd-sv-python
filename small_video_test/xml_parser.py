from xml.sax.handler import ContentHandler
from xml.sax import make_parser, parse

class HeadlineHandler(ContentHandler):
    in_headline = False
    def __init__(self, headlines_list):
        ContentHandler.__init__(self)
        self.headlines = headlines_list
        self.data = []
        self.init_count = 0
        self.start_count = 0
        self.end_count = 0
        self.c3 = 0
        print("c0: %d" % self.init_count )

    def startElement(self, name, attrs):
        self.start_count += 1
        print("start_count : %d, start: %s" % (self.start_count, name))
        # print("key: ", attrs.keys() )
        # print("value: ", attrs["value"])
        if name == 'h1':
            self.in_headline = True
        if name == "param":
            print(attrs["key"])
            print(attrs["value"])


    def endElement(self, name):
        self.end_count += 1
        print("end_count : %d, end: %s" % (self.end_count, name))
        if name == 'h1':
            text = ''.join(self.data)
            self.data = []
            self.headlines.append(text)
            self.in_headline = False

    def characters(self, content):
        self.c3 += 1
        print("characters : %d, content: %s" % (self.c3, content))
        if self.in_headline:
            self.data.append(content)
            print("in_headline %s" % content)


if __name__ == '__main__':
    headlines = []
    parse('website.xml', HeadlineHandler(headlines_list=headlines))
    for h in headlines:
        print(h)
