import struct
import hashlib
import os
import csv

hashSize = 17035841
fileName = "data/201803_BolsaFamilia_Pagamentos.csv"
fileName2 = "data/HashBolsaFevNovo.dat"
indexName = "data/IntersecMesesF.dat"
indexFormat = "11sLL"
keyColumnIndex = 5

indexStruct = struct.Struct(indexFormat)
 
def h(key):
    global hashSize
    return int(hashlib.sha1(key).hexdigest(), 16) % hashSize

f = open(fileName,"r")
f1 = open(fileName2,"rb")
f2 = open(indexName,"wb")

saida = f.readline()
f2.write(saida)

while True:
    linha = f.readline()

    if linha == "":
        break
    record = linha.split(";")
    nis = record[5][1:-1]
    p = h(nis)
    offset = p * indexStruct.size
    i = 1
    while True:
        f1.seek(offset, os.SEEK_SET)
        x = f1.read(indexStruct.size)
        indexRecord = indexStruct.unpack(x)
        if indexRecord[0] == str(nis):
        	f2.write(linha)
        	break
        offset = indexRecord[2]
        if offset == 0:
            break
        i += 1

f.close()
f1.close()
f2.close()
