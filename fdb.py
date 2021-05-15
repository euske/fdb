#!/usr/bin/env python
import sys
import os
import time
import stat
import uuid
import hashlib
import os.path
import sqlite3
import mimetypes
import logging
import shutil
import re

def time2str(t):
    return time.strftime('%Y-%m-%d %H:%M:%S', t)

WORDS = re.compile(r'\w+', re.U)
def getwords(s):
    return WORDS.findall(s)

def identify_mplayer(path):
    from subprocess import Popen, PIPE, DEVNULL
    args = (
        'mplayer', '-really-quiet', '-noconfig', 'all',
        '-vo', 'null', '-ao', 'null', '-frames', '0',
        '-identify', path)
    attrs = {}
    try:
        p = Popen(args, stdin=DEVNULL, stdout=PIPE, stderr=DEVNULL, encoding='utf-8')
        a = {}
        ck = None
        for line in p.stdout:
            (k,_,v) = line.strip().partition('=')
            a[k] = v
            if k == 'ID_LENGTH':
                attrs['length'] = v
            elif k == 'ID_VIDEO_WIDTH':
                attrs['width'] = v
            elif k == 'ID_VIDEO_HEIGHT':
                attrs['height'] = v
            elif k == 'ID_VIDEO_FPS':
                attrs['fps'] = v
            elif k.startswith('ID_CLIP_INFO_NAME') and v == 'creation_time':
                ck = 'ID_CLIP_INFO_VALUE'+k[17:]
        if ck is not None:
            t = time.strptime(a[ck][:19], '%Y-%m-%dT%H:%M:%S')
            attrs['datetime'] = time2str(t)
    except OSError:
        pass
    return list(attrs.items())

EXIF_TAGS = ('ImageDescription', 'Model', 'DateTimeOriginal', 'DateTime')
def identify_pil(path):
    from PIL import Image, ExifTags, UnidentifiedImageError
    attrs = {}
    try:
        img = Image.open(path)
        exif = img.getexif()
        for (k,v) in exif.items():
            k = ExifTags.TAGS.get(k)
            if k == 'ImageDescription':
                attrs['description'] = v
            elif k == 'Model':
                attrs['model'] = v
            elif k == 'DateTime' or k == 'DateTimeOriginal':
                t = time.strptime(v, '%Y:%m:%d %H:%M:%S')
                attrs['datetime'] = time2str(t)
    except (OSError, UnidentifiedImageError):
        pass
    return list(attrs.items())

MDB_DEFS = '''
CREATE TABLE IF NOT EXISTS Entries (
  entryId INTEGER PRIMARY KEY,
  fileName TEXT,
  fileType TEXT,
  fileSize INTEGER,
  fileHash TEXT,
  dateAdded TEXT);

CREATE TABLE IF NOT EXISTS Attrs (
  entryId INTEGER,
  attrName TEXT,
  attrValue TEXT);

CREATE TABLE IF NOT EXISTS Logs (
  actionId INTEGER PRIMARY KEY,
  entryId INTEGER,
  timestamp TEXT,
  action TEXT);
'''

class FileDB:

    BUFSIZE = 1024*1024
    MDB_NAME = 'metadata.db'

    def __init__(self, basedir, dryrun=0):
        self.basedir = basedir
        self.dryrun = dryrun
        self.mdb = sqlite3.connect(os.path.join(basedir, self.MDB_NAME))
        self._init_mdb()
        self._cur = self.mdb.cursor()
        return

    def close(self):
        self.mdb.commit()
        return

    def _init_mdb(self):
        self.mdb.executescript(MDB_DEFS)
        return

    def _get_path(self, name):
        assert 2 < len(name)
        prefix = name[:2]
        dirpath = os.path.join(self.basedir, prefix)
        if not os.path.exists(dirpath):
            os.makedirs(dirpath)
        path = os.path.join(dirpath, name)
        return path

    def _get_entry(self, eid):
        for (filename, filetype, filesize) in cur.execute(
                'SELECT fileName, fileType, fileSize FROM Entries'
                ' WHERE entryId=?;',
                (eid,)):
            return (filename, filetype, filesize)
        raise KeyError(eid)

    def _add_entry(self, path):
        logging.debug(f'add_entry: {path}')
        cur = self._cur
        h = hashlib.sha1()
        filesize = 0
        with open(path, 'rb') as fp:
            while True:
                data = fp.read(self.BUFSIZE)
                filesize += len(data)
                if not data: break
                h.update(data)
        filehash = h.hexdigest()
        for (eid,filetype) in cur.execute(
                'SELECT entryId, fileType FROM Entries'
                ' WHERE fileSize=? AND fileHash=?;',
                (filesize, filehash)):
            return (None, filetype, eid)
        else:
            (_,ext) = os.path.splitext(path)
            filename = uuid.uuid4().hex + ext.lower()
            (filetype,_) = mimetypes.guess_type(path)
            cur.execute(
                'INSERT INTO Entries VALUES (NULL, ?, ?, ?, ?, datetime());',
                (filename, filetype, filesize, filehash))
            eid = cur.lastrowid
            return (filename, filetype, eid)

    def _add_attrs(self, eid, attrs):
        logging.debug(f'add_attrs: {eid} {attrs}')
        for (name, value) in attrs:
            self._cur.execute(
                'INSERT INTO Attrs VALUES (?, ?, ?);',
                (eid, name, value))
        return

    def _add_log(self, eid, action):
        self._cur.execute(
            'INSERT INTO Logs VALUES (NULL, ?, datetime(), ?);',
            (eid, action))
        return

    def add(self, path):
        logging.info(f'adding: {path!r}...')
        (name, filetype, eid) = self._add_entry(path)
        if name is None: return
        if not self.dryrun:
            dst = self._get_path(name)
            shutil.copyfile(path, dst)
        st = os.stat(path)
        attrs = [
            ('path', path),
            ('ctime', time2str(time.gmtime(st[stat.ST_CTIME]))),
            ('mtime', time2str(time.gmtime(st[stat.ST_MTIME]))),
        ]
        for w in getwords(path):
            attrs.append(('tag', w.lower()))
        if filetype is None:
            pass
        elif filetype.startswith('video/') or filetype.startswith('audio/'):
            attrs.extend(identify_mplayer(path))
        elif filetype.startswith('image/'):
            attrs.extend(identify_pil(path))
        self._add_attrs(eid, attrs)
        return


def main(argv):
    import getopt
    def usage():
        print(f'usage: {argv[0]} '
              '[-v] [-n] basedir {add|remove|list|show|tag} [args ...]')
        return 100
    try:
        (opts, args) = getopt.getopt(argv[1:], 'vno:')
    except getopt.GetoptError:
        return usage()
    level = logging.INFO
    dryrun = False
    output = None
    for (k, v) in opts:
        if k == '-v': level = logging.DEBUG
        elif k == '-n': dryrun = True
        elif k == '-o': output = v
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=level)

    if not args: return usage()
    basedir = args.pop(0)
    db = FileDB(basedir, dryrun=dryrun)
    cmd = 'list'
    if args:
        cmd = args.pop(0)
    if cmd == 'add':
        for arg in args:
            if os.path.isfile(arg):
                db.add(arg)
            elif os.path.isdir(arg):
                for (dirpath,dirnames,filenames) in os.walk(arg):
                    for name in filenames:
                        if name.startswith('.'): continue
                        path = os.path.join(dirpath, name)
                        db.add(path)
    elif cmd == 'remove':
        pass
    elif cmd == 'list':
        db.list(args)
    elif cmd == 'show':
        db.show(args)
    elif cmd == 'tag':
        db.tag(args)
    else:
        usage()
    db.close()
    return

if __name__ == '__main__': sys.exit(main(sys.argv))
