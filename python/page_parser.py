from lxml import etree
from urllib.parse import urljoin

class PageParser:
    def __init__(self, owner, url):
        self.owner = owner
        self.base = url
        self.found_base = False
                
    def parse_links(self, fp):
        # limit memory usage
        context = etree.iterparse(fp, events=('end',), tag=('a', 'base'), html=True, recover=True)
        for action, elem in context:
            if not self.found_base and (elem.tag == 'base'):
                parent = elem.getparent()[0]
                if parent is not None and (parent.tag == 'head'):
                    grandparent = parent.getparent()[0]
                    if grandparent is not None and (grandparent.tag == 'html'):
                        self.found_base = True
                        href = elem.get('href')
                        if href:
                            self.base = urljoin(self.base, href)
            elif elem.tag == 'a':
                href = elem.get('href')
                if href:
                    link = urljoin(self.base, href)
                    self.owner.add_link(link)
                
            # cleanup
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]
