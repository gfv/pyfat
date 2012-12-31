# coding: utf-8
import re
import struct
import datetime
from pprint import pprint as pp

__author__ = 'gfv'

class FilePointer:
    def __init__(self, stream, offset=0):
        if isinstance(stream, FilePointer):
            self.stream = stream.stream
            self.offset = stream.offset
        else:
            self.stream = stream
            self.offset = offset

    def __add__(self, other):
        if isinstance(other, int):
            return FilePointer(self.stream, self.offset + other)
        elif isinstance(other, FilePointer):
            return FilePointer(self.stream, self.offset + other.offset)
        raise TypeError

    def __sub__(self, other):
        if isinstance(other, int):
            return FilePointer(self.stream, self.offset - other)
        elif isinstance(other, FilePointer):
            return FilePointer(self.stream, self.offset - other.offset)
        raise TypeError

    def read(self, length):
        self.stream.seek(self.offset)
        return self.stream.read(length)

    def interpret(self, fmt):
        rawdata = self.read(struct.calcsize(fmt))
        return struct.unpack(fmt, rawdata)

class BPB:
    def __init__(self, ptr):
        self.BytsPerSec, self.SecPerClus, self.RsvdSecCnt, \
            self.NumFATs, self.RootEntCnt, self.TotSec16, self.Media, self.FATSz16, \
            self.SecPerTrk, self.NumHeads, self.HiddSec, self.TotSec32 = ptr.interpret("<11xHBHBHHBHHHII")

    def populate32(self, ptr):
        self.FATSz32, self.ExtFlags, self.FSVer, self.RootClus, self.FSInfo, self.BKBootSec, \
            self.Reserved, self.DrvNum, self.BootSig, self.VolID = (ptr + 36).interpret("<IHHIHH12xBBBI11x8x")

    def populate16(self, ptr):
        self.DrvNum, self.BootSig, self.VolID = (ptr + 36).interpret("<BxBI11x8x")


class ClusterStorage (object):
    def __init__(self, base, cluster_length):
        self.base = base
        self.cluster_length = cluster_length

    def __getitem__(self, cluster):
        return (self.base + ((cluster - 2)* self.cluster_length)).read(self.cluster_length)

#
# FAT objects
#
class FAT(object):
    def __init__(self, ptr):  self.ptr = ptr

class FAT32 (FAT):
    def follows(self, cluster): return (self.ptr + cluster*4).interpret("<I")[0] & 0x0fffffff
    def __getitem__(self, cluster): return self.follows(cluster)

class FAT16 (FAT):
    def follows(self, cluster): return (self.ptr + cluster*2).interpret("<H")[0]
    def __getitem__(self, cluster): return self.follows(cluster)


#
# File system objects.
#
class BasicFS (object):
    def __init__(self, bpb, start):
        self.bpb = bpb
        self.start = start
        self.fat_offset = bpb.RsvdSecCnt * bpb.BytsPerSec

    def read_root(self):
        return None

    def read_file(self, directory_entry):
        return FileChain(directory_entry.cluster, directory_entry.size, self.fat, self.clusters)

    def read_directory(self, directory_entry):
        if directory_entry.cluster == 0:
            return self.read_root()
        return DirectoryChain(directory_entry.cluster, self.fat, self.clusters)

    def read_root(self):
        return self.root_directory

    def find_object(self, seeked, directory_ptr):
        in_this_directory = len(seeked) == 1

        for f in directory_ptr.entries():
            pp(f.name.lower())
            if f.name.lower() == seeked[0].lower():
                if in_this_directory: return f
                return self.find_object(seeked[1:], self.read_directory(f))
        return None

    def dump_file_chain(self, chain, out_fname):
        w = open(out_fname, "wb")
        for part in self.read_file(chain):
            w.write(part)
        w.close()

    def print_dir_contents(self, directory_ptr):
        d = self.read_directory(directory_ptr)
        for f in d.entries():
            print f.name, f.cluster

class FS32 (BasicFS):
    def __init__(self, bpb, start):
        super(FS32, self).__init__(bpb, start)
        self.bpb.populate32(start)

        cluster_offset = self.fat_offset + (bpb.NumFATs * bpb.FATSz32) * bpb.BytsPerSec
        cluster_length = bpb.BytsPerSec * bpb.SecPerClus
        self.clusters = ClusterStorage(start + cluster_offset, cluster_length)
        self.fat = FAT32(start + self.fat_offset)

        self.root_directory = DirectoryChain(self.bpb.RootClus, self.fat, self.clusters)


class FS16 (BasicFS):
    def __init__(self, bpb, start):
        super(FS16, self).__init__(bpb, start)
        self.bpb.populate16(start)
        self.fat = FAT16(start + self.fat_offset)

        self.root_directory_ptr = start + self.fat_offset + bpb.NumFATs * bpb.FATSz16 * bpb.BytsPerSec

        cluster_length = bpb.BytsPerSec * bpb.SecPerClus
        root_dir_length = bpb.RootEntCnt * 32

        cluster_ptr = self.root_directory_ptr + root_dir_length
        self.clusters = ClusterStorage(cluster_ptr, cluster_length)

        self.root_directory = FS16RootDirectory(self.root_directory_ptr, self.bpb.RootEntCnt)

class FileList(object):
    def entries(self): pass

class ClusterChain (object):
    def __init__(self, cluster, fat, clusters):
        self.cluster = cluster
        self.clusters = clusters
        self.fat = fat

        if isinstance(fat, FAT16):
            self.stop = 0xffff
        elif isinstance(fat, FAT32):
            self.stop = 0x0fffffff

    def _iterate_clusters(self):
        c_cluster = self.cluster
        yield self.clusters[c_cluster]
        while self.fat.follows(c_cluster) != self.stop:
            c_cluster = self.fat.follows(c_cluster)
            yield self.clusters[c_cluster]

class FS16RootDirectory(object):
    def __init__(self, ptr, number):
        self.ptr = ptr
        self.number = number
        self.cluster = 0

    def entries(self):
        lfn = u""

        for i in xrange(0, self.number):
            entry = DirectoryEntry(self.ptr.read(32))
            self.ptr = self.ptr + 32
            entry, lfn = DirectoryChain._assemble_entry(entry, lfn)
            if entry is not None:
                yield entry
            elif lfn is None:
                return


class DirectoryChain(ClusterChain, FileList):
    def __init__(self, cluster, fat, clusters):
        super(DirectoryChain, self).__init__(cluster, fat, clusters)

    @staticmethod
    def _assemble_entry(entry, assembled_lfn):
        if entry.ignore: return (None, assembled_lfn)
        if entry.end: return (None, None)

        if entry.LFN is not None:
            assembled_lfn = entry.LFN + assembled_lfn
            return (None, assembled_lfn)

            # если мы досюда дошли, то это -- короткое имя файла
        if assembled_lfn != u"":
            entry.name = assembled_lfn.replace(chr(0x00), '')
            assembled_lfn = u""
        else:
            if entry.is_directory or entry.is_volume_label:
                entry.name = entry.Filename + entry.Extension
            else:
                entry.name = entry.Filename + '.' + entry.Extension

        return (entry, assembled_lfn)


    def entries(self):
        lfn = u""
        entries_left = True

        for cluster in self._iterate_clusters():
            for i in xrange(0, self.clusters.cluster_length / 32):
                entry = DirectoryEntry(cluster[32*i : 32*(i+1)])
                entry, lfn = self._assemble_entry(entry, lfn)
                if entry is not None:
                    yield entry
                elif lfn is None:
                    return


class DirectoryEntry (object):
    def __init__(self, record):
        self.ignore = record[0] == chr(0xe5)
        self.end = record[0] == chr(0x00)

        self.Filename = record[0:8].strip()
        self.Extension = record[8:11].replace(' ', '')
        self.Attribute, self.Case, self.Cms, self.Ctime, self.Cdate, self.Adate, \
            self.StartClusterHigh, self.Wtime, self.Wdate, self.StartClusterLow, \
            self.size = struct.unpack("<BBBHHHHHHHI", record[11:])


        self.LFN = None

        if self.Attribute == 0x0f:
            self.LFN = (record[1:11] + record[14:26] + record[28:32]).replace(chr(0xff), '').decode('utf-16').strip()
        elif not self.end:
            self.creation_date = self._parse_time(self.Cdate, self.Ctime, self.Cms)
            self.last_write_date = self._parse_time(self.Wdate, self.Wtime)
            self.last_access_date = self._parse_time(self.Adate)

        self.is_directory = self.Attribute & 0x10
        self.is_volume_label = self.Attribute & 0x08

        self.is_readonly = self.Attribute & 0x01
        self.is_hidden = self.Attribute & 0x02
        self.is_system = self.Attribute & 0x04

        self.cluster = (self.StartClusterHigh << 16) + self.StartClusterLow

    @staticmethod
    def _parse_time(date, time=0, ms=0):
        day = date & 0x1f
        month = (date >> 5) & 0x0f
        year = 1980 + (date >> 9)

        microsecond = (ms % 100) * 1000
        second = (time & 0x1f) + (ms / 100)
        minute = (time >> 5) & 0x3f
        hour = time >> 11

        return datetime.datetime(year, month, day, hour, minute, second, microsecond)


class FileChain(ClusterChain):
    def __init__(self, cluster, size, fat, clusters):
        super(FileChain, self).__init__(cluster, fat, clusters)
        self.size = size

    def parts(self):
        bytes_left = self.size
        for seg in self._iterate_clusters():
            if bytes_left > self.clusters.cluster_length:
                yield seg
            else:
                yield seg[:bytes_left]

            bytes_left -= self.clusters.cluster_length


def load_fs(image_start):
    bpb = BPB(image_start)
    if bpb.RootEntCnt == 0:
        return FS32(bpb, image_start)
    else:
        return FS16(bpb, image_start)

docs = """python FAT16/32 reader commands:
h - help
ls - list current directory
cd <dir> - cd to <dir>
cat <filename> - dumps <filename> from image to console
cp <filename> <external> - copies <filename> from image to an external file specified by <external>"""

if __name__ == '__main__':
    fpath = raw_input('fat image file: ')

    try:
        fat = open(fpath, "rb")
        image_start = FilePointer(fat, 0)
        fs = load_fs(image_start)
        root_dir = fs.read_root()
        cdir = root_dir

        cmd = raw_input('cmd (h for help)> ').strip().lower().split(None, 1)
        while cmd[0] != 'q':
            if cmd[0] == 'h' or cmd[0] == 'help':
                print docs
            elif cmd[0] == 'ls':
                fs.print_dir_contents(cdir)
            elif cmd[0] == 'cd':
                path = cmd[1].split('/\\')
                obj = fs.find_object(path, cdir)
                if obj is None:
                    print "no such directory :("
                elif not obj.is_directory:
                    print "not a directory"
                else:
                    cdir = fs.read_directory(obj)
            elif cmd[0] == 'cat':
                path = cmd[1].split('/\\')
                obj = fs.find_object(path, cdir)
                if obj is None:
                    print "no such file :("
                elif obj.is_directory or obj.is_volume_label:
                    print "not a file"
                else:
                    for segment in fs.read_file(obj).parts():
                        print segment
            elif cmd[0] == 'cp':
                mtch = re.match('("[^"]+"|[^" ]+) ("[^"]+"|[^" ]+)', cmd[1])
                if mtch is not None:
                    src = mtch.group(1).strip(' "')
                    dst = mtch.group(2).strip(' "')
                    obj = fs.find_object(src, cdir)
                    w = open(dst, "wb")
                    if obj is None:
                        print "no such file :("
                    elif obj.is_directory or obj.is_volume_label:
                        print "not a file"
                    else:
                        for segment in fs.read_file(obj).parts():
                            w.write(segment)
                else:
                    print "invalid arguments"


            cmd = raw_input('cmd (h for help)> ').strip().lower().split(None, 1)


    except IOError:
        print "could not open image file"