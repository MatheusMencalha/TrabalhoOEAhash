import struct
import hashlib
import os
import csv

hashSize = 17035841
fileName = "data/201802_BolsaFamilia_Pagamentos.csv"
indexName = "data/HashBolsaFevNovo.dat"
indexFormat = "11sLL"
keyColumnIndex = 5

indexStruct = struct.Struct(indexFormat)


def h(key):
    global hashSize
    return int(hashlib.sha1(key).hexdigest(), 16) % hashSize


fi = open(indexName, "wb")
emptyIndexRecord = indexStruct.pack("".encode('utf-8'), 0, 0)
for i in range(0, hashSize):
    fi.write(emptyIndexRecord)
fi.close()

f = open(fileName, "r")
fi = open(indexName, "r+b")

fi.seek(0, os.SEEK_END)
fileIndexSize = fi.tell()
print ("IndexFileSize", fileIndexSize)


recordNumber = 0
while True:

    line = f.readline()

    if line == "":
        break
    record = line.split(";")
    nis = record[5][1:-1]

    p = h(nis)
    fi.seek(p * indexStruct.size, os.SEEK_SET)
    indexRecord = indexStruct.unpack(fi.read(indexStruct.size))
    fi.seek(p * indexStruct.size, os.SEEK_SET)
    if indexRecord[0][0] == "\0":
        fi.write(indexStruct.pack(nis, recordNumber, 0))
    else:
        nextPointer = indexRecord[2]
        fi.write(indexStruct.pack(indexRecord[0],indexRecord[1],fileIndexSize))
        fi.seek(0,os.SEEK_END)
        fi.write(indexStruct.pack(nis,recordNumber,nextPointer))
        fileIndexSize = fi.tell()
    recordNumber += 1
f.close()
fi.close()