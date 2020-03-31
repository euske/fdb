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
import shutil

def time2str(t):
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(t))

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

    BUFSIZ = 1024*1024
    MDB_NAME = 'metadata.db'

    def __init__(self, basedir):
        self.basedir = basedir
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

    def _add_entry(self, path):
        cur = self._cur
        h = hashlib.sha1()
        filesize = 0
        with open(path, 'rb') as fp:
            while True:
                data = fp.read(self.BUFSIZ)
                filesize += len(data)
                if not data: break
                h.update(data)
        filehash = h.hexdigest()
        for (eid,) in cur.execute(
                'SELECT entryId FROM Entries WHERE fileSize=? AND fileHash=?;',
                (filesize, filehash)):
            return (None, eid)
        else:
            (_,ext) = os.path.splitext(path)
            filename = uuid.uuid4().hex + ext.lower()
            (filetype,_) = mimetypes.guess_type(path)
            cur.execute(
                'INSERT INTO Entries VALUES (NULL, ?, ?, ?, ?, datetime());',
                (filename, filetype, filesize, filehash))
            eid = cur.lastrowid
            return (filename, eid)

    def _add_attr(self, eid, name, value):
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
        (name, eid) = self._add_entry(path)
        if name is not None:
            dst = self._get_path(name)
            shutil.copyfile(path, dst)
        self._add_attr(eid, 'path', path)
        st = os.stat(path)
        self._add_attr(eid, 'ctime', time2str(st[stat.ST_CTIME]))
        self._add_attr(eid, 'mtime', time2str(st[stat.ST_MTIME]))
        return


def main(argv):
    import getopt
    def usage():
        print('usage: %s [-d] basedir {add|remove|list|show|tag} [args ...]]' % argv[0])
        return 100
    try:
        (opts, args) = getopt.getopt(argv[1:], 'do:')
    except getopt.GetoptError:
        return usage()
    debug = 0
    output = None
    for (k, v) in opts:
        if k == '-d': debug += 1
        elif k == '-o': output = v
    if not args: return usage()
    basedir = args.pop(0)
    db = FileDB(basedir)
    cmd = 'list'
    if args:
        cmd = args.pop(0)
    if cmd == 'add':
        for arg in args:
            for (dirpath,dirnames,filenames) in os.walk(arg):
                for name in filenames:
                    if name.startswith('.'): continue
                    path = os.path.join(dirpath, name)
                    print('adding: %r...' % path)
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
        return usage()
    db.close()
    return

if __name__ == '__main__': sys.exit(main(sys.argv))
