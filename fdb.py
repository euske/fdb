#!/usr/bin/env python
##
##  usage:
##     ./fdb.py dir add *.jpg *.mp3
##
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
import getopt
import json
import re
from io import BytesIO
from PIL import Image, ExifTags, UnidentifiedImageError
from subprocess import Popen, PIPE, DEVNULL

def time2str(t):
    return time.strftime('%Y-%m-%d %H:%M:%S', t)
def str2time(t):
    return time.strptime(t, '%Y-%m-%d %H:%M:%S')

WORDS = re.compile(r'\w+', re.U)
def get_words(text):
    return [ w.lower() for w in WORDS.findall(text) ]

def get_filehash(path, bufsize=1024*1024):
    filesize = 0
    h = hashlib.sha1()
    with open(path, 'rb') as fp:
        while True:
            data = fp.read(bufsize)
            filesize += len(data)
            if not data: break
            h.update(data)
    filehash = h.hexdigest()
    return (filesize, filehash)

def get_thumbnail(img, size):
    img.thumbnail(size)
    fp = BytesIO()
    img.save(fp, format='jpeg')
    return fp.getvalue()

def identify_video(path, position=0, thumb_size=(128,128)):
    timestamp = None
    attrs = { 'duration':0, 'width':0, 'height':0 }
    thumbnail = None
    args = (
        'ffprobe', '-of', 'json', '-show_format', '-show_streams',
        path)
    try:
        p = Popen(args, stdin=DEVNULL, stdout=PIPE, stderr=DEVNULL, encoding='utf-8')
        obj = json.load(p.stdout)
        fmt = obj['format']
        if 'duration' in fmt:
            attrs['duration'] = int(float(fmt['duration'])+.5)
        if 'tags' in fmt:
            tags = fmt['tags']
            if 'creation_time' in tags:
                v = tags['creation_time']
                t = time.strptime(v[:19], '%Y-%m-%dT%H:%M:%S')
                timestamp = time2str(t)
        for strm in obj['streams']:
            if 'width' in strm:
                v = strm['width']
                attrs['width'] = v
            if 'height' in strm:
                v = strm['height']
                attrs['height'] = v
        p.wait()
    except OSError:
        pass
    args = (
        'ffmpeg', '-y', '-ss', str(position),
        '-i', path, '-f', 'image2', '-vframes', '1', '-')
    try:
        p = Popen(args, stdin=DEVNULL, stdout=PIPE, stderr=DEVNULL, encoding=None)
        data = p.stdout.read()
        p.wait()
        img = Image.open(BytesIO(data))
        thumbnail = get_thumbnail(img, thumb_size)
    except OSError:
        pass
    return (timestamp, attrs, thumbnail)

def identify_image(path, thumb_size=(128,128)):
    timestamp = None
    attrs = {}
    thumbnail = None
    try:
        img = Image.open(path)
        attrs['width'] = img.width
        attrs['height'] = img.height
        exif = img.getexif()
        rotation = 0
        for (k,v) in exif.items():
            k = ExifTags.TAGS.get(k)
            if k == 'ImageDescription':
                attrs['description'] = v
            elif k == 'Orientation':
                if v == 8:
                    rotation = 90
                elif v == 3:
                    rotation = 180
                elif v == 6:
                    rotation = 270
                attrs['rotation'] = rotation
            elif k == 'DateTime' or k == 'DateTimeOriginal':
                t = time.strptime(v, '%Y:%m:%d %H:%M:%S')
                timestamp = time2str(t)
        thumbnail = get_thumbnail(img.rotate(rotation), thumb_size)
    except (OSError, UnidentifiedImageError):
        pass
    return (timestamp, attrs, thumbnail)

MDB_DEFS = '''
CREATE TABLE IF NOT EXISTS Entries (
  entryId INTEGER PRIMARY KEY,
  timestamp TEXT,
  fileName TEXT,
  fileType TEXT,
  fileSize INTEGER,
  fileHash TEXT);

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

    MDB_NAME = 'metadata.db'
    THUMB_SIZE = (128,128)

    def __init__(self, basedir, dryrun=0):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.basedir = basedir
        self.origdir = os.path.join(basedir, 'orig')
        os.makedirs(self.origdir, exist_ok=True)
        self.thumbdir = os.path.join(basedir, 'thumb')
        os.makedirs(self.thumbdir, exist_ok=True)
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

    def _get_path(self, subdir, name):
        assert 2 < len(name)
        prefix = name[:2]
        dirpath = os.path.join(subdir, prefix)
        if not os.path.exists(dirpath):
            os.makedirs(dirpath)
        path = os.path.join(dirpath, name)
        return path

    def _get_entry(self, eid):
        for (timestamp, filename, filetype, filesize) in self._cur.execute(
                'SELECT timestamp, fileName, fileType, fileSize FROM Entries'
                ' WHERE entryId=?;',
                (eid,)):
            return (timestamp, filename, filetype, filesize)
        raise KeyError(eid)

    def _list_entry(self, query):
        cur = self.mdb.cursor()
        sql = 'SELECT entryId, timestamp, fileType, fileSize FROM Entries ORDER BY timestamp DESC;'
        try:
            for (eid, timestamp, filetype, filesize) in cur.execute(sql):
                attrs = self._get_attrs(eid)
                yield (eid, timestamp, filetype, filesize, attrs)
        finally:
            cur.close()
        return

    def _get_attrs(self, eid):
        attrs = []
        for (attrName, attrValue) in self._cur.execute(
                'SELECT attrName, attrValue FROM Attrs'
                ' WHERE entryId=?;',
                (eid,)):
            attrs.append((attrName, attrValue))
        return attrs

    def _add_entry(self, path):
        self.logger.debug(f'add_entry: {path}')
        cur = self._cur
        (filesize, filehash) = get_filehash(path)
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
                'INSERT INTO Entries VALUES (NULL, NULL, ?, ?, ?, ?);',
                (filename, filetype, filesize, filehash))
            eid = cur.lastrowid
            return (filename, filetype, eid)

    def _add_attrs(self, eid, attrs):
        self.logger.debug(f'add_attrs: {eid} {attrs}')
        for (name, value) in attrs:
            self._cur.execute(
                'INSERT INTO Attrs VALUES (?, ?, ?);',
                (eid, name, str(value)))
        return

    def _add_log(self, eid, action):
        self._cur.execute(
            'INSERT INTO Logs VALUES (NULL, ?, datetime(), ?);',
            (eid, action))
        return

    def add(self, path, tags=None):
        (filename, filetype, eid) = self._add_entry(path)
        if filename is None:
            self.logger.info(f'ignored: {path!r}...')
            return
        self.logger.info(f'adding: {path!r}...')
        if not self.dryrun:
            dst = self._get_path(self.origdir, filename)
            shutil.copyfile(path, dst)
        attrs = [('path', path)]
        for w in get_words(path):
            attrs.append(('tag', w))
        for t in (tags or []):
            attrs.append(('tag', t))
        timestamp = thumbnail = None
        if filetype is None:
            pass
        elif filetype.startswith('video/') or filetype.startswith('audio/'):
            (timestamp, attrs1, thumbnail) = identify_video(
                path, thumb_size=self.THUMB_SIZE)
            attrs.extend(attrs1.items())
        elif filetype.startswith('image/'):
            (timestamp, attrs1, thumbnail) = identify_image(
                path, thumb_size=self.THUMB_SIZE)
            attrs.extend(attrs1.items())
        if timestamp is None:
            st = os.stat(path)
            timestamp = time2str(time.gmtime(st[stat.ST_CTIME]))
        self._cur.execute(
            'UPDATE Entries SET timestamp=? WHERE entryId=?;',
            (timestamp, eid))
        attrs.append(('timestamp', timestamp))
        self._add_attrs(eid, attrs)
        if not self.dryrun and thumbnail is not None:
            (name,_) = os.path.splitext(filename)
            dst = self._get_path(self.thumbdir, name+'.jpg')
            with open(dst, 'wb') as fp:
                fp.write(thumbnail)
        self._add_log(eid, 'add')
        return

    def list(self, args):
        for (eid, timestamp, filetype, filesize, attrs) in self._list_entry(args):
            tags = [ v for (k,v) in attrs if k == 'tag' ]
            attrs = dict(attrs)
            a = []
            if 'width' and 'height' in attrs:
                a.append(f'({attrs["width"]}x{attrs["height"]})')
            if 'duration' in attrs:
                a.append(f'[{attrs["duration"]}s]')
            if 'descriotion' in attrs:
                a.append(attrs['description'])
            a.append('{'+', '.join(tags)+'}')
            print(timestamp, filetype, filesize, ' '.join(a))
        return

    def help(self):
        print(f'help:')
        print(f'  add [-t tag] files ...')
        return 100

    def run(self, args):
        cmd = 'list'
        if args:
            cmd = args.pop(0)
        if cmd == 'add':
            try:
                (opts, args) = getopt.getopt(args, 't:')
            except getopt.GetoptError:
                return self.help()
            tags = []
            for (k, v) in opts:
                if k == '-t': tags.append(v)
            for arg in args:
                if os.path.isfile(arg):
                    self.add(arg, tags)
                elif os.path.isdir(arg):
                    for (dirpath,dirnames,filenames) in os.walk(arg):
                        for name in filenames:
                            if name.startswith('.'): continue
                            path = os.path.join(dirpath, name)
                            self.add(path, tags)
        elif cmd == 'remove':
            pass
        elif cmd == 'list':
            self.list(args)
        elif cmd == 'show':
            self.show(args)
        elif cmd == 'tag':
            self.tag(args)
        else:
            self.help()
        return


def main(argv):
    def usage():
        print(f'usage: {argv[0]} '
              '[-v] [-n] basedir {add|remove|list|show|tag} [args ...]')
        return 100
    try:
        (opts, args) = getopt.getopt(argv[1:], 'vn')
    except getopt.GetoptError:
        return usage()
    level = logging.INFO
    dryrun = False
    for (k, v) in opts:
        if k == '-v': level = logging.DEBUG
        elif k == '-n': dryrun = True
    logging.basicConfig(format='%(asctime)s %(levelname)s %(name)s %(message)s', level=level)

    if not args: return usage()
    basedir = args.pop(0)
    db = FileDB(basedir, dryrun=dryrun)
    try:
        db.run(args)
    finally:
        db.close()
    return

if __name__ == '__main__': sys.exit(main(sys.argv))
