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
from http import HTTPStatus
from http.server import HTTPServer
from http.server import SimpleHTTPRequestHandler
from PIL import Image, ImageOps, ExifTags, UnidentifiedImageError
from subprocess import Popen, PIPE, DEVNULL

def time2str(t):
    return time.strftime('%Y-%m-%d %H:%M:%S', t)
def str2time(t):
    return time.strptime(t, '%Y-%m-%d %H:%M:%S')

WORDS = re.compile(r'\w+', re.U)
def get_words(text):
    return [ w.lower() for w in WORDS.findall(text) ]

def get_filehash(path, bufsize=1024*1024):
    h = hashlib.sha1()
    with open(path, 'rb') as fp:
        while True:
            data = fp.read(bufsize)
            if not data: break
            h.update(data)
    return h.hexdigest()

def get_thumbnail(img, size):
    img.thumbnail(size)
    fp = BytesIO()
    img.save(fp, format='jpeg')
    return fp.getvalue()

def get_thumb_video(path, position=0, thumb_size=(128,128)):
    args = (
        'ffmpeg', '-y', '-ss', str(position),
        '-i', path, '-f', 'image2', '-vframes', '1', '-')
    try:
        p = Popen(args, stdin=DEVNULL, stdout=PIPE, stderr=DEVNULL, encoding=None)
        data = p.stdout.read()
        p.wait()
        img = Image.open(BytesIO(data))
        return get_thumbnail(img, thumb_size)
    except OSError:
        return None

def get_thumb_image(path, thumb_size=(128,128)):
    try:
        img = Image.open(path)
        img = ImageOps.exif_transpose(img)
        return get_thumbnail(img, thumb_size)
    except (OSError, UnidentifiedImageError):
        return None

def get_attrs_video(path):
    attrs = { 'duration':0, 'width':0, 'height':0 }
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
                attrs['timestamp'] = time2str(t)
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
    return attrs

def get_attrs_image(path):
    attrs = {}
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
                attrs['timestamp'] = time.strptime(v, '%Y:%m:%d %H:%M:%S')
    except (OSError, UnidentifiedImageError):
        pass
    return attrs

MDB_DEFS = '''
CREATE TABLE IF NOT EXISTS Entries (
  entryId INTEGER PRIMARY KEY,
  fileId TEXT,
  fileType TEXT,
  fileSize INTEGER,
  fileHash TEXT);

CREATE TABLE IF NOT EXISTS Attrs (
  entryId INTEGER,
  attrName TEXT,
  attrValue TEXT);

CREATE INDEX IF NOT EXISTS AttrsIndex ON Attrs (
  entryId, attrName);

CREATE TABLE IF NOT EXISTS Logs (
  actionId INTEGER PRIMARY KEY,
  entryId INTEGER,
  timestamp TEXT,
  action TEXT);
'''

class DBRequestHandler(SimpleHTTPRequestHandler):

    DB = None

    class HTTPError(OSError):
        def __init__(self, code):
            self.code = code

    def do_GET(self):
        try:
            self.path = self.convert_path(self.path)
            rs = self.headers.get('Range')
            if rs:
                (fp, offset, nbytes) = self.send_head_partial(rs)
                try:
                    fp.seek(offset)
                    data = fp.read(nbytes)
                    self.wfile.write(data)
                finally:
                    fp.close()
                return
            SimpleHTTPRequestHandler.do_GET(self)
        except self.HTTPError as e:
            self.send_error(e.code)
        return

    def do_HEAD(self):
        try:
            self.path = self.convert_path(self.path)
            rs = self.headers.get('Range')
            if rs:
                try:
                    (fp, _, _) = self.send_head_partial(rs)
                    fp.close()
                except self.HTTPError as e:
                    self.send_error(e.code)
                return
            SimpleHTTPRequestHandler.do_HEAD(self)
        except self.HTTPError as e:
            self.send_error(e.code)
        return

    def convert_path(self, path):
        assert path.startswith('/')
        (category,_,fileid) = path[1:].partition('/')
        if category == 'orig':
            return self.DB.get_path(self.DB.origdir, fileid)
        elif category == 'thumb':
            return self.DB.get_path(self.DB.thumbdir, fileid)
        else:
            raise self.HTTPError(HTTPStatus.BAD_REQUEST)

    RANGE = re.compile(r'bytes=(\d+)?-(\d+)?', re.I)

    def send_head_partial(self, rs):
        m = self.RANGE.match(rs)
        if not m:
            raise self.HTTPError(HTTPStatus.BAD_REQUEST)
        (s,e) = m.groups()
        if s is None and e is None:
            raise self.HTTPError(HTTPStatus.BAD_REQUEST)
        path = self.translate_path(self.path)
        ctype = self.guess_type(path)
        try:
            fp = open(path, 'rb')
        except OSError:
            raise self.HTTPError(HTTPStatus.NOT_FOUND)
        fs = os.fstat(fp.fileno())
        length = fs[6]
        if s is None:
            s = length-int(e)
            e = length
        elif e is None:
            s = int(s)
            e = length
        else:
            s = int(s)
            e = int(e)+1
        nbytes = e-s
        try:
            self.send_response(HTTPStatus.PARTIAL_CONTENT)
            self.send_header('Content-Type', ctype)
            self.send_header('Content-Length', str(nbytes))
            self.send_header('Content-Range', 'bytes %d-%d/%d' % (s,e-1,length))
            self.send_header('Last-Modified', self.date_time_string(fs.st_mtime))
            self.end_headers()
        except:
            fp.close()
            raise
        return (fp, s, nbytes)

    def end_headers(self):
        self.send_header('Accept-Ranges', 'bytes')
        SimpleHTTPRequestHandler.end_headers(self)
        return


class FileDB:

    MDB_NAME = 'metadata.db'
    THUMB_SIZE = (128,128)

    class DuplicateEntry(Exception): pass

    def __init__(self, basedir, strict=False, dryrun=False):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.basedir = basedir
        self.origdir = os.path.join(basedir, 'orig')
        os.makedirs(self.origdir, exist_ok=True)
        self.thumbdir = os.path.join(basedir, 'thumb')
        os.makedirs(self.thumbdir, exist_ok=True)
        self.strict = strict
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

    def get_path(self, subdir, fileid):
        assert 2 < len(fileid)
        prefix = fileid[:2]
        dirpath = os.path.join(subdir, prefix)
        if not os.path.exists(dirpath):
            os.makedirs(dirpath)
        path = os.path.join(dirpath, fileid)
        return path

    def get_entry(self, eid):
        for (fileid, filetype, filesize, timestamp) in self._cur.execute(
                'SELECT fileId, fileType, fileSize, attrValue'
                ' FROM Entries, Attrs'
                ' WHERE Entries.entryId=? AND Entries.entryId=Attrs.entryId'
                ' AND Attrs.attrName="timestamp";',
                (eid,)):
            return (fileid, filetype, filesize, timestamp)
        raise KeyError(eid)

    def list_entry(self, query):
        cur = self.mdb.cursor()
        sql = ('SELECT Entries.entryId, fileId, fileType, fileSize, attrValue'
               ' FROM Entries, Attrs'
               ' WHERE Entries.entryId=Attrs.entryId'
               ' AND Attrs.attrName="timestamp" ORDER BY attrValue DESC;')
        try:
            for (eid, fileid, filetype, filesize, timestamp) in cur.execute(sql):
                attrs = self._get_attrs(eid)
                yield (eid, fileid, filetype, filesize, timestamp, attrs)
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

    def _add_entry(self, srcpath, relpath):
        self.logger.debug(f'add_entry: {srcpath} {relpath}')
        cur = self._cur
        st = os.stat(srcpath)
        filesize = st[stat.ST_SIZE]
        if not self.strict:
            mtime = st[stat.ST_MTIME]
            for (eid,) in cur.execute(
                    'SELECT Entries.entryId FROM Entries, Attrs AS A1, Attrs AS A2'
                    ' WHERE Entries.entryId=A1.entryId AND Entries.entryId=A2.entryId'
                    ' AND Entries.fileSize=?'
                    ' AND A1.attrName="path" AND A1.attrValue=?'
                    ' AND A2.attrName="mtime" AND A2.attrValue=?',
                    (filesize, relpath, mtime)):
                raise self.DuplicateEntry(eid)
        filehash = get_filehash(srcpath)
        for (eid,) in cur.execute(
                'SELECT entryId FROM Entries'
                ' WHERE fileSize=? AND fileHash=?;',
                (filesize, filehash)):
            raise self.DuplicateEntry(eid)
        (_,ext) = os.path.splitext(srcpath)
        fileid = uuid.uuid4().hex + ext.lower()
        (filetype,_) = mimetypes.guess_type(srcpath)
        cur.execute(
            'INSERT INTO Entries VALUES (NULL, ?, ?, ?, ?);',
            (fileid, filetype, filesize, filehash))
        eid = cur.lastrowid
        return (fileid, filetype, eid)

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

    def add(self, basedir, relpath, tags=None):
        srcpath = os.path.join(basedir, relpath)
        try:
            (fileid, filetype, eid) = self._add_entry(srcpath, relpath)
        except self.DuplicateEntry:
            self.logger.info(f'ignored: {relpath!r}...')
            return
        self.logger.info(f'adding: {relpath!r}...')
        if not self.dryrun:
            dstpath = self.get_path(self.origdir, fileid)
            shutil.copyfile(srcpath, dstpath)
        st = os.stat(srcpath)
        mtime = st[stat.ST_MTIME]
        attrs = [('path', relpath), ('mtime', mtime)]
        for w in get_words(relpath):
            attrs.append(('tag', w))
        for t in (tags or []):
            attrs.append(('tag', t))
        attrs1 = {}
        thumbnail = None
        if filetype is None:
            pass
        elif filetype.startswith('video/') or filetype.startswith('audio/'):
            attrs1 = get_attrs_video(srcpath)
            thumbnail = get_thumb_video(srcpath, thumb_size=self.THUMB_SIZE)
        elif filetype.startswith('image/'):
            attrs1 = get_attrs_image(srcpath)
            thumbnail = get_thumb_image(srcpath, thumb_size=self.THUMB_SIZE)
        if 'timestamp' not in attrs1:
            attrs1['timestamp'] = time2str(time.gmtime(st[stat.ST_CTIME]))
        attrs.extend(attrs1.items())
        self._add_attrs(eid, attrs)
        if not self.dryrun and thumbnail is not None:
            (name,_) = os.path.splitext(fileid)
            dstpath = self.get_path(self.thumbdir, name+'.jpg')
            with open(dstpath, 'wb') as fp:
                fp.write(thumbnail)
        self._add_log(eid, 'add')
        return

    def list(self, args):
        for (eid, _, filetype, filesize, timestamp, attrs) in self.list_entry(args):
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

    def server(self, port=8080):
        DBRequestHandler.DB = self
        DBRequestHandler.protocol_version = 'HTTP/1.1'
        server_address = ('', port)
        with HTTPServer(server_address, DBRequestHandler) as httpd:
            sa = httpd.socket.getsockname()
            (host, port) = (sa[0], sa[1])
            self.logger.info(f'Serving HTTP on {host} port {port} (http://{host}:{port}/) ...')
            try:
                httpd.serve_forever()
            except KeyboardInterrupt:
                self.logger.info('\nKeyboard interrupt received, exiting.')
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
                    self.add('.', arg, tags)
                elif os.path.isdir(arg):
                    for (dirpath,dirnames,filenames) in os.walk(arg):
                        basepath = os.path.relpath(dirpath, arg)
                        for name in filenames:
                            if name.startswith('.'): continue
                            relpath = os.path.join(basepath, name)
                            self.add(arg, relpath, tags)
        elif cmd == 'remove':
            pass
        elif cmd == 'list':
            self.list(args)
        elif cmd == 'show':
            self.show(args)
        elif cmd == 'tag':
            self.tag(args)
        elif cmd == 'server':
            try:
                (opts, args) = getopt.getopt(args, 'p:')
            except getopt.GetoptError:
                return self.help()
            port = 8080
            for (k, v) in opts:
                if k == '-p': port = int(v)
            self.server(port=port)
        else:
            self.help()
        return


def main(argv):
    def usage():
        print(f'usage: {argv[0]} '
              '[-v] [-n] basedir {add|remove|list|show|tag|server} [args ...]')
        return 100
    try:
        (opts, args) = getopt.getopt(argv[1:], 'vsn')
    except getopt.GetoptError:
        return usage()
    level = logging.INFO
    strict = False
    dryrun = False
    for (k, v) in opts:
        if k == '-v': level = logging.DEBUG
        elif k == '-s': strict = True
        elif k == '-n': dryrun = True
    logging.basicConfig(format='%(asctime)s %(levelname)s %(name)s %(message)s', level=level)

    if not args: return usage()
    basedir = args.pop(0)
    db = FileDB(basedir, strict=strict, dryrun=dryrun)
    try:
        db.run(args)
    finally:
        db.close()
    return

if __name__ == '__main__': sys.exit(main(sys.argv))
