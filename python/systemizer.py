from datetime import datetime
import locale
import re
from itemizer import Itemizer

join_rx = re.compile("joined Twitter on\\s+(.+)")

not_number_rx = re.compile("[^0-9]")

class Systemizer(Itemizer):
    def __init__(self):
        Itemizer.__init__(self)
        locale.setlocale(locale.LC_ALL, "C") # to parse English dates

    def get_profile_name(self, root):
        name_attrs = root.xpath("//td[@class='user-info']/div[@class='fullname']/text()")
        for attr in name_attrs:
            return attr.strip()

        return None

    def get_profile_following(self, root):
        return self.get_profile_stat(root, "following")

    def get_profile_followers(self, root):
        return self.get_profile_stat(root, "followers")

    def get_quarry_since(self, root):
        text_nodes = root.xpath("//h2/text()")
        for txt in text_nodes:
            m = join_rx.search(txt)
            if m:
                tail = m.group(1)
                return datetime.strptime(tail, "%A %B %d, %Y")

        return None

    def get_profile_stat(self, root, segment):
        url_path = '/' + segment
        path = "//a[substring(@href, string-length(@href) - string-length('%s') + 1) = '%s']/div[@class='statnum']/text()" % (url_path, url_path)
        value_attrs = root.xpath(path)
        for attr in value_attrs:
            plain = not_number_rx.sub("", attr)
            return int(plain)
