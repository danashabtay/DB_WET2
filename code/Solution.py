from typing import List
import Utility.DBConnector as Connector
from Utility.ReturnValue import ReturnValue
from Utility.Exceptions import DatabaseException
from Business.Photo import Photo
from Business.RAM import RAM
from Business.Disk import Disk
from psycopg2 import sql


def createTables():
    table1Name = "Photo"
    table2Name = "Disk"
    table3Name = "RAM"
    table4Name = "PhotoOnDisk"
    table5Name = "RAMOnDisk"
    attributeList = []

    attributeList.append(("PhotoID", "INT NOT NULL UNIQUE CHECK(PhotoID>0)"))
    attributeList.append(("Description", "TEXT NOT NULL"))
    attributeList.append(("DiskSizeNeeded", "INT NOT NULL CHECK(DiskSizeNeeded>=0)"))
    attributeList.append(("PRIMARY KEY", "(PhotoID)"))
    createTable(table1Name, attributeList)
    attributeList.clear()

    attributeList.append(("DiskID", "INT NOT NULL UNIQUE CHECK(DiskID>0)"))
    attributeList.append(("ManufacturingCompany", "TEXT NOT NULL"))
    attributeList.append(("Speed", "INT NOT NULL CHECK(Speed>0)"))
    attributeList.append(("FreeSpace", "INT NOT NULL CHECK(FreeSpace>=0)"))
    attributeList.append(("CostPerByte", "INT NOT NULL CHECK(CostPerByte>0)"))
    attributeList.append(("PRIMARY KEY", "(DiskID)"))
    createTable(table2Name, attributeList)
    attributeList.clear()

    attributeList.append(("RAMID", "INT NOT NULL UNIQUE CHECK(RAMID>0)"))
    attributeList.append(("Size", "INT NOT NULL CHECK(Size>0)"))
    attributeList.append(("Company", "TEXT NOT NULL"))
    attributeList.append(("PRIMARY KEY", "(RAMID)"))
    createTable(table3Name, attributeList)
    attributeList.clear()

    attributeList.append(("PhotoID", "INT"))
    attributeList.append(("DiskID", "INT"))
    attributeList.append(("FORIEGN KEY", "(PhotoID) REFERENCES"+table1Name+"(PhotoID) ON DELETE CASCADE ON UPDATE "
                                                                           "CASCADE"))
    attributeList.append(("FORIEGN KEY", "(DiskID) REFERENCES"+table2Name+"(DiskID) ON DELETE CASCADE ON UPDATE "
                                                                          "CASCADE"))
    createTable(table4Name, attributeList)
    attributeList.clear()

    attributeList.append(("RAMID", "INT"))
    attributeList.append(("DiskID", "INT"))
    attributeList.append(("FORIEGN KEY", "(RAMID) REFERENCES"+table3Name+"(RAMID) ON DELETE CASCADE ON UPDATE "
                                                                           "CASCADE"))
    attributeList.append(("FORIEGN KEY", "(DiskID) REFERENCES"+table2Name+"(DiskID) ON DELETE CASCADE ON UPDATE "
                                                                          "CASCADE"))
    createTable(table5Name, attributeList)
    attributeList.clear()

    #ADD VIEWS HERE!!!!!!!!!!!!

def createTable(name, attributes: list):
    conn = Connector.DBConnector()
    try:
        message =""
        for item in attributes :
            for subitem in item :
                message += subitem + " "
            message += ","
        message = message[:-1]
        conn.execute("CREATE TABLE "+name+"(" + message + ")"+";")
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    finally:
        try:
            conn.close()
        except DatabaseException.ConnectionInvalid as e:
            print(e)









def clearTables():
    pass


def dropTables():
    pass


def addPhoto(photo: Photo) -> ReturnValue:
    return ReturnValue.OK


def getPhotoByID(photoID: int) -> Photo:
    return Photo()


def deletePhoto(photo: Photo) -> ReturnValue:
    return ReturnValue.OK


def addDisk(disk: Disk) -> ReturnValue:
    return ReturnValue.OK


def getDiskByID(diskID: int) -> Disk:
    return Disk()


def deleteDisk(diskID: int) -> ReturnValue:
    return ReturnValue.OK


def addRAM(ram: RAM) -> ReturnValue:
    return ReturnValue.OK


def getRAMByID(ramID: int) -> RAM:
    return RAM()


def deleteRAM(ramID: int) -> ReturnValue:
    return ReturnValue.OK


def addDiskAndPhoto(disk: Disk, photo: Photo) -> ReturnValue:
    return ReturnValue.OK


def addPhotoToDisk(photo: Photo, diskID: int) -> ReturnValue:
    return ReturnValue.OK


def removePhotoFromDisk(photo: Photo, diskID: int) -> ReturnValue:
    return ReturnValue.OK


def addRAMToDisk(ramID: int, diskID: int) -> ReturnValue:
    return ReturnValue.OK


def removeRAMFromDisk(ramID: int, diskID: int) -> ReturnValue:
    return ReturnValue.OK


def averagePhotosSizeOnDisk(diskID: int) -> float:
    return 0


def getTotalRamOnDisk(diskID: int) -> int:
    return 0


def getCostForDescription(description: str) -> int:
    return 0


def getPhotosCanBeAddedToDisk(diskID: int) -> List[int]:
    return []


def getPhotosCanBeAddedToDiskAndRAM(diskID: int) -> List[int]:
    return []


def isCompanyExclusive(diskID: int) -> bool:
    return True


def isDiskContainingAtLeastNumExists(description : str, num : int) -> bool:
    return True


def getDisksContainingTheMostData() -> List[int]:
    return []


def getConflictingDisks() -> List[int]:
    return []


def mostAvailableDisks() -> List[int]:
    return []


def getClosePhotos(photoID: int) -> List[int]:
    return []

if __name__ == '__main__':
    createTables()