import os
import zipfile
from common import get_loose_path, get_volume_path

class VolumeHolder:
    def __init__(self):
        self.volume_id = None
        self.zp = None

    def open_page(self, url_id, volume_id=None):
        f = None
        if volume_id is None:
            loose_path = get_loose_path(url_id)
            if os.path.exists(loose_path):
                f = open(loose_path, "rb")
        else:
            if volume_id != self.volume_id:
                self.change_volume(volume_id)

            try:
                info = self.zp.getinfo(str(url_id))
                f = self.zp.open(info)
            except KeyError:
                pass

        return f
    
    def open_headers(self, url_id, volume_id=None):
        f = None
        if volume_id is None:
            loose_path = get_loose_path(url_id, True)
            if os.path.exists(loose_path):
                f = open(loose_path, "rb")
        else:
            if volume_id != self.volume_id:
                self.change_volume(volume_id)

            try:
                info = self.zp.getinfo(str(url_id) + 'h')
                f = self.zp.open(info)
            except KeyError:
                pass

        return f

    def get_body_size(self, url_id, volume_id=None):
        sz = None
        if volume_id is None:
            loose_path = get_loose_path(url_id)
            if os.path.exists(loose_path):
                statinfo = os.stat(loose_path)
                sz = statinfo.st_size
        else:
            if volume_id != self.volume_id:
                self.change_volume(volume_id)

            try:
                info = self.zp.getinfo(str(url_id))
                sz = info.file_size
            except KeyError:
                pass

        return sz
    
    def change_volume(self, volume_id):
        if self.zp is not None:
            self.zp.close()

        archive_path = get_volume_path(volume_id)
        self.zp = zipfile.ZipFile(archive_path)
        self.volume_id = volume_id
        
    def close(self):
        if self.zp is not None:
            self.zp.close()
            self.zp = None
