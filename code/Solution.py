from typing import List
import Utility.DBConnector as Connector
from Utility.ReturnValue import ReturnValue
from Utility.Exceptions import DatabaseException
from Business.Photo import Photo
from Business.RAM import RAM
from Business.Disk import Disk
from psycopg2 import sql

PHOTO_TABLE = "photoTable"
DISK_TABLE = "DiskTable"
RAM_TABLE = "RAMTable"
DISK_AND_PHOTO = "DiskAndPhotoTable"
RAM_AND_DISK = "RAMAndDiskTable"

tablenames={
    "photoTable":"Photo",
    "DiskTable":"Disk",
    "RAMTable":"RAM",
    "DiskAndPhotoTable":"PhotoOnDisk",
    "RAMAndDiskTable":"RAMOnDisk"
}
def tryToClose (connection):
    try:
        conn.close()
    except DatabaseException as e:
        print(e)

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
    except DatabaseException as e:
        print(e)
    finally:
        tryToClose(conn)


def createTables():
    attributeList = []

    attributeList.append(("PhotoID", "INT NOT NULL UNIQUE CHECK(PhotoID>0)"))
    attributeList.append(("Description", "TEXT NOT NULL"))
    attributeList.append(("DiskSizeNeeded", "INT NOT NULL CHECK(DiskSizeNeeded>=0)"))
    attributeList.append(("PRIMARY KEY", "(PhotoID)"))
    createTable(tablenames["photoTable"], attributeList)
    attributeList.clear()

    attributeList.append(("DiskID", "INT NOT NULL UNIQUE CHECK(DiskID>0)"))
    attributeList.append(("ManufacturingCompany", "TEXT NOT NULL"))
    attributeList.append(("Speed", "INT NOT NULL CHECK(Speed>0)"))
    attributeList.append(("FreeSpace", "INT NOT NULL CHECK(FreeSpace>=0)"))
    attributeList.append(("CostPerByte", "INT NOT NULL CHECK(CostPerByte>0)"))
    attributeList.append(("PRIMARY KEY", "(DiskID)"))
    createTable(tablenames["DiskTable"], attributeList)
    attributeList.clear()

    attributeList.append(("RAMID", "INT NOT NULL UNIQUE CHECK(RAMID>0)"))
    attributeList.append(("Size", "INT NOT NULL CHECK(Size>0)"))
    attributeList.append(("Company", "TEXT NOT NULL"))
    attributeList.append(("PRIMARY KEY", "(RAMID)"))
    createTable(tablenames["RAMTable"], attributeList)
    attributeList.clear()

    attributeList.append(("PhotoID", "INT"))
    attributeList.append(("DiskID", "INT"))
    attributeList.append(("FORIEGN KEY", "(PhotoID) REFERENCES"+tablenames["photoTable"]+"(PhotoID) ON DELETE CASCADE ON UPDATE "
                                                                          + "CASCADE"))
    attributeList.append(("FORIEGN KEY", "(DiskID) REFERENCES"+tablenames["DiskTable"]+"(DiskID) ON DELETE CASCADE ON UPDATE "
                                                                         + "CASCADE"))
    createTable(tablenames["DiskAndPhotoTable"], attributeList)
    attributeList.clear()

    attributeList.append(("RAMID", "INT"))
    attributeList.append(("DiskID", "INT"))
    attributeList.append(("FORIEGN KEY", "(RAMID) REFERENCES"+tablenames["RAMTable"]+"(RAMID) ON DELETE CASCADE ON UPDATE "
                                                                          + "CASCADE"))
    attributeList.append(("FORIEGN KEY", "(DiskID) REFERENCES"+tablenames["DiskTable"]+"(DiskID) ON DELETE CASCADE ON UPDATE "
                                                                         + "CASCADE"))
    createTable(tablenames["RAMAndDiskTable"], attributeList)
    attributeList.clear()

   #ADD VIEWS HERE!!!!!!!!!!!!
def clearTables():
    conn = Connector.DBConnector()
    message = ""
    try:
        for table in tablenames.keys() :
            message+=f"DELETE FROM {table}; \n"
        conn.execute(message)
        conn.commit()
    except DatabaseException as e:
        print(e)
    finally:
        tryToClose(conn)
        
def dropTables():
    conn = Connector.DBConnector()
    message = ""
    try:
        for table in tablenames.keys() :
            message+=f"DROP TABLE {table} CASCADE ; \n"
        conn.execute(message)
        conn.commit()
    except DatabaseException as e:
        print(e)
    finally:
        tryToClose(conn)

#ADD VIEWS HERE!!!!!!!!!!!!

def addPhoto(photo: Photo) -> ReturnValue:
    #the following checks could be replaced by a check violation exception
    if photo.getPhotoID <= 0 or photo.getSize <0 :
        return ReturnValue.BAD_PARAMS
    if photo.getPhotoID is None or photo.getDescription is None or photo.getSize is None :
        return ReturnValue.BAD_PARAMS 
    message = f"INSERT INTO {tablenames[PHOTO_TABLE]}"+
        "(PhotoID , Description, DiskSizeNeeded)"+
        f" VALUES ({sql.literal(photo.getPhotoID)}, {sql.literal(photo.getDescription)} ,{sql.literal(photo.getDescription)}" 
    conn = Connector.DBConnector()
    try:
        conn.execute(message)
        conn.commit()
    except DatabaseException.UNIQUE_VIOLATION as e:
        tryToClose(conn)
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException as e:
        print(e)
        tryToClose(conn)       
        return ReturnValue.ERROR
    finally:
        tryToClose(conn)
        return ReturnValue.OK

def getPhotoByID(photoID: int) -> Photo:
    conn=Connector.DBConnector()
    message = f"SELECT * FROMP{tablenames[PHOTO_TABLE]} WHERE PhotoID = {photoID}"
    try:
        affected,answer = conn.execute(message)
        conn.commit()
    except DatabaseException as e:
        print("should not be possible but here we are\n"+ e)
        tryToClose(conn)
        return Photo.badPhoto()
    tryToClose(conn) 
    if answer.isEmpty() :
        return Photo.badPhoto()
    #not sure what to do with a answer now, printing in the meantime
    print(answer)
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