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
        connection.close()
    except DatabaseException as e:
        print(e)

def createTableMessage(name, attributes: list):
    message = ""
    for item in attributes:
        for subitem in item:
            message += subitem + " "
        message += ","
    message = message[:-1]
    return "CREATE TABLE "+name+"(" + message + ")"+";"


def createTables():
    conn = Connector.DBConnector()
    attributeList = []

    attributeList.append(("PhotoID", "INT NOT NULL UNIQUE CHECK(PhotoID>0)"))
    attributeList.append(("Description", "TEXT NOT NULL"))
    attributeList.append(("DiskSizeNeeded", "INT NOT NULL CHECK(DiskSizeNeeded>=0)"))
    attributeList.append(("PRIMARY KEY", "(PhotoID)"))
    message1 = createTableMessage(tablenames["photoTable"], attributeList)
    attributeList.clear()

    attributeList.append(("DiskID", "INT NOT NULL UNIQUE CHECK(DiskID>0)"))
    attributeList.append(("ManufacturingCompany", "TEXT NOT NULL"))
    attributeList.append(("Speed", "INT NOT NULL CHECK(Speed>0)"))
    attributeList.append(("FreeSpace", "INT NOT NULL CHECK(FreeSpace>=0)"))
    attributeList.append(("CostPerByte", "INT NOT NULL CHECK(CostPerByte>0)"))
    attributeList.append(("PRIMARY KEY", "(DiskID)"))
    message2 = createTableMessage(tablenames["DiskTable"], attributeList)
    attributeList.clear()

    attributeList.append(("RAMID", "INT NOT NULL UNIQUE CHECK(RAMID>0)"))
    attributeList.append(("Company", "TEXT NOT NULL"))
    attributeList.append(("Size", "INT NOT NULL CHECK(Size>0)"))
    attributeList.append(("PRIMARY KEY", "(RAMID)"))
    message3 = createTableMessage(tablenames["RAMTable"], attributeList)
    attributeList.clear()

    attributeList.append(("PhotoID", "INT NOT NULL"))
    attributeList.append(("DiskID", "INT NOT NULL"))
    attributeList.append(("PRIMARY KEY", "(PhotoID, DiskID)"))
    attributeList.append(("FOREIGN KEY", "(PhotoID) REFERENCES "+tablenames["photoTable"]+" (PhotoID) ON DELETE CASCADE ON UPDATE "
                                                                          + "CASCADE"))
    attributeList.append(("FOREIGN KEY", "(DiskID) REFERENCES "+tablenames["DiskTable"]+" (DiskID) ON DELETE CASCADE ON UPDATE "
                                                                         + "CASCADE"))
    message4 = createTableMessage(tablenames["DiskAndPhotoTable"], attributeList)
    attributeList.clear()

    attributeList.append(("RAMID", "INT NOT NULL"))
    attributeList.append(("DiskID", "INT NOT NULL"))
    attributeList.append(("PRIMARY KEY", "(RAMID, DiskID)"))
    attributeList.append(("FOREIGN KEY", "(RAMID) REFERENCES "+tablenames["RAMTable"]+" (RAMID) ON DELETE CASCADE ON UPDATE "
                                                                          + "CASCADE"))
    attributeList.append(("FOREIGN KEY", "(DiskID) REFERENCES "+tablenames["DiskTable"]+" (DiskID) ON DELETE CASCADE ON UPDATE "
                                                                         + "CASCADE"))
    message5 = createTableMessage(tablenames["RAMAndDiskTable"], attributeList)
    attributeList.clear()

    try:
        conn.execute(message1 + message2 + message3 + message4 + message5)
        conn.commit()
    except DatabaseException as e:
        print(e)

    tryToClose(conn)

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

    tryToClose(conn)
        
def dropTables():
    conn = Connector.DBConnector()
    message = ""
    try:
        for table in tablenames.values() :
            message+=f"DROP TABLE IF EXISTS {table} CASCADE ; \n"
        conn.execute(message)
        conn.commit()
    except DatabaseException as e:
        print(e)

    tryToClose(conn)

#ADD VIEWS HERE!!!!!!!!!!!!

def addPhoto(photo: Photo) -> ReturnValue:
    message = f"""INSERT INTO {tablenames[PHOTO_TABLE]}(PhotoID, Description, DiskSizeNeeded) 
    VALUES({photo.getPhotoID()}, {photo.getDescription()}, {photo.getSize()});"""

    #need to revisit to change "" around decription accordingly!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

    try:
        conn = Connector.DBConnector()
        print(message)
        rows, values = conn.execute(message)
        print(values)
        conn.commit()
    except DatabaseException.UNIQUE_VIOLATION as e:
        tryToClose(conn)
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.CHECK_VIOLATION as e:
        tryToClose(conn)
        return ReturnValue.BAD_PARAMS
    except DatabaseException.NOT_NULL_VIOLATION as e:
        tryToClose(conn)
        return ReturnValue.BAD_PARAMS
    except DatabaseException as e:
        print(e)
        tryToClose(conn)       
        return ReturnValue.ERROR

    tryToClose(conn)
    return ReturnValue.OK

def getPhotoByID(photoID: int) -> Photo:
    conn = Connector.DBConnector()
    message = f"SELECT * FROM {tablenames[PHOTO_TABLE]} WHERE PhotoID = {photoID};"
    try:
        affected, answer = conn.execute(message)
        conn.commit()
    except DatabaseException as e:
        print("should not be possible but here we are\n" + e)
        tryToClose(conn)
        return Photo.badPhoto()

    tryToClose(conn)
    if answer.isEmpty():
        return Photo.badPhoto()
    photoResult = Photo(answer.rows[0][0], answer.rows[0][1], answer.rows[0][2])
    return photoResult


def deletePhoto(photo: Photo) -> ReturnValue:
    conn = Connector.DBConnector()
    message1 = f"""UPDATE {tablenames[DISK_TABLE]} SET FreeSpace = FreeSpace+{photo.getSize()} WHERE DiskID = (SELECT DiskID 
               FROM {tablenames[DISK_AND_PHOTO]} WHERE PhotoID = {photo.getPhotoID()});"""

    #return here after adding photo+disk!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

    message2 = f"DELETE FROM {tablenames[PHOTO_TABLE]} WHERE PhotoID = {photo.getPhotoID()};"

    try:
        conn.execute(message1 + message2)
        conn.commit()
    except DatabaseException as e:
        tryToClose(conn)
        return ReturnValue.ERROR

    tryToClose(conn)
    return ReturnValue.OK


def addDisk(disk: Disk) -> ReturnValue:
    message = f"""INSERT INTO {tablenames[DISK_TABLE]}(DiskID, ManufacturingCompany, Speed, FreeSpace, CostPerByte) 
        VALUES({disk.getDiskID()}, {disk.getCompany()}, {disk.getSpeed()}, {disk.getFreeSpace()}, {disk.getCost()});"""

    # need to revisit to change "" around company accordingly!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

    try:
        conn = Connector.DBConnector()
        print(message)
        rows, values = conn.execute(message)
        print(values)
        conn.commit()
    except DatabaseException.UNIQUE_VIOLATION as e:
        tryToClose(conn)
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.CHECK_VIOLATION as e:
        tryToClose(conn)
        return ReturnValue.BAD_PARAMS
    except DatabaseException.NOT_NULL_VIOLATION as e:
        tryToClose(conn)
        return ReturnValue.BAD_PARAMS
    except DatabaseException as e:
        print(e)
        tryToClose(conn)
        return ReturnValue.ERROR

    tryToClose(conn)
    return ReturnValue.OK


def getDiskByID(diskID: int) -> Disk:
    conn = Connector.DBConnector()
    message = f"SELECT * FROM {tablenames[DISK_TABLE]} WHERE DiskID = {diskID};"
    try:
        affected, answer = conn.execute(message)
        conn.commit()
    except DatabaseException as e:
        print("should not be possible but here we are\n" + e)
        tryToClose(conn)
        return Disk.badDisk()

    tryToClose(conn)
    if answer.isEmpty():
        return Disk.badDisk()
    diskResult = Disk(answer.rows[0][0], answer.rows[0][1], answer.rows[0][2], answer.rows[0][3], answer.rows[0][4])
    return diskResult


def deleteDisk(diskID: int) -> ReturnValue:
    conn = Connector.DBConnector()
    message = f"DELETE FROM {tablenames[DISK_TABLE]} WHERE DiskID = {diskID};"
    try:
        rows, values = conn.execute(message)
        conn.commit()
        if rows == 0:
            tryToClose(conn)
            return ReturnValue.NOT_EXISTS
    except DatabaseException as e:
        tryToClose(conn)
        return ReturnValue.ERROR

    tryToClose(conn)
    return ReturnValue.OK

def addRAM(ram: RAM) -> ReturnValue:
    message = f"""INSERT INTO {tablenames[RAM_TABLE]}(RAMID,Company,Size) 
    VALUES({ram.getRamID()}, {ram.getCompany()}, {ram.getSize()});"""

    #need to revisit to change "" around company accordingly!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

    try:
        conn = Connector.DBConnector()
        print(message)
        rows, values = conn.execute(message)
        print(values)
        conn.commit()
    except DatabaseException.UNIQUE_VIOLATION as e:
        tryToClose(conn)
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.CHECK_VIOLATION as e:
        tryToClose(conn)
        return ReturnValue.BAD_PARAMS
    except DatabaseException.NOT_NULL_VIOLATION as e:
        tryToClose(conn)
        return ReturnValue.BAD_PARAMS
    except DatabaseException as e:
        print(e)
        tryToClose(conn)
        return ReturnValue.ERROR

    tryToClose(conn)
    return ReturnValue.OK


def getRAMByID(ramID: int) -> RAM:
    conn = Connector.DBConnector()
    message = f"SELECT * FROM {tablenames[RAM_TABLE]} WHERE RAMID = {ramID};"
    try:
        affected, answer = conn.execute(message)
        conn.commit()
    except DatabaseException as e:
        print("should not be possible but here we are\n" + e)
        tryToClose(conn)
        return RAM.badRAM()

    tryToClose(conn)
    if answer.isEmpty():
        return RAM.badRAM()
    RAMResult = RAM(answer.rows[0][0], answer.rows[0][1], answer.rows[0][2])
    return RAMResult


def deleteRAM(ramID: int) -> ReturnValue:
    conn = Connector.DBConnector()
    message1 = f"""UPDATE {tablenames[DISK_TABLE]} 
    SET FreeSpace = FreeSpace+(SELECT Size FROM {tablenames[RAM_TABLE]} WHERE RAMID = {ramID}) 
    WHERE DiskID = (SELECT DiskID FROM {tablenames[RAM_AND_DISK]} WHERE RAMID = {ramID});"""
    print(message1)
    # return here after adding ram+disk!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

    message2 = f"DELETE FROM {tablenames[RAM_TABLE]} WHERE RAMID = {ramID};"
    print(message2)
    try:
        rows, values = conn.execute(message1 + message2)
        conn.commit()
        if rows == 0:
            tryToClose(conn)
            return ReturnValue.NOT_EXISTS
    except DatabaseException as e:
        tryToClose(conn)
        return ReturnValue.ERROR

    tryToClose(conn)
    return ReturnValue.OK


def addDiskAndPhoto(disk: Disk, photo: Photo) -> ReturnValue:
    message1 = f"""INSERT INTO {tablenames[PHOTO_TABLE]}(PhotoID, Description, DiskSizeNeeded) 
        VALUES({photo.getPhotoID()}, {photo.getDescription()}, {photo.getSize()});"""
    message2 = f"""INSERT INTO {tablenames[DISK_TABLE]}(DiskID, ManufacturingCompany, Speed, FreeSpace, CostPerByte) 
            VALUES({disk.getDiskID()}, {disk.getCompany()}, {disk.getSpeed()}, {disk.getFreeSpace()}, {disk.getCost()});"""

    # need to revisit to change "" around decription accordingly!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

    try:
        conn = Connector.DBConnector()
        print(message1 + message2)
        rows, values = conn.execute(message1 + message2)
        print(values)
        conn.commit()
    except DatabaseException.UNIQUE_VIOLATION as e:
        tryToClose(conn)
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException as e:
        print(e)
        tryToClose(conn)
        return ReturnValue.ERROR

    tryToClose(conn)
    return ReturnValue.OK



def addPhotoToDisk(photo: Photo, diskID: int) -> ReturnValue:
    message1 = f"""INSERT INTO {tablenames[DISK_AND_PHOTO]}(PhotoID, DiskID) 
    VALUES({photo.getPhotoID()}, {diskID});"""
    message2 = f"""UPDATE {tablenames[DISK_TABLE]} SET Freespace = Freespace - {photo.getSize()} 
    WHERE DiskID={diskID};"""
    print(message1+message2)
    try:
        conn = Connector.DBConnector()
        rows, values = conn.execute(message1 + message2)
        conn.commit()
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        print(1)
        tryToClose(conn)
        return ReturnValue.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        tryToClose(conn)
        return ReturnValue.NOT_EXISTS
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(2)
        tryToClose(conn)
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.CHECK_VIOLATION as e:
        print(5)
        tryToClose(conn)
        return ReturnValue.BAD_PARAMS
    except DatabaseException as e:
        print(3)
        conn.rollback()
        tryToClose(conn)
        return ReturnValue.ERROR

    print(4)
    tryToClose(conn)
    return ReturnValue.OK


def removePhotoFromDisk(photo: Photo, diskID: int) -> ReturnValue:
    message1 = f"""DELETE FROM {tablenames[DISK_AND_PHOTO]} 
        WHERE PhotoID={photo.getPhotoID()} AND DiskID={diskID};"""
    message2 = f"""UPDATE {tablenames[DISK_TABLE]} SET Freespace = Freespace + {photo.getSize()} 
        WHERE DiskID={diskID};"""
    print(message1 + message2)
    try:
        conn = Connector.DBConnector()
        rows, values = conn.execute(message1 + message2)
        conn.commit()
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print("shouldnt be happening!!!!!!!!!!!!!!!!!!!!:(")
    except DatabaseException as e:
        print(3)
        conn.rollback()
        tryToClose(conn)
        return ReturnValue.ERROR

    return ReturnValue.OK


def addRAMToDisk(ramID: int, diskID: int) -> ReturnValue:
    message1 = f"""INSERT INTO {tablenames[RAM_AND_DISK]}(RAMID, DiskID) VALUES({ramID}, {diskID});"""
    message2 = f"""UPDATE {tablenames[DISK_TABLE]} SET Freespace = Freespace - (SELECT Size FROM 
    {tablenames[RAM_TABLE]} WHERE RAMID = {ramID}) WHERE DiskID={diskID};"""
    print(message1+message2)
    try:
        conn = Connector.DBConnector()
        rows, values = conn.execute(message1+message2)
        conn.commit()
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        tryToClose(conn)
        return ReturnValue.NOT_EXISTS
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(2)
        tryToClose(conn)
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException as e:
        print(3)
        conn.rollback()
        tryToClose(conn)
        return ReturnValue.ERROR

    print(4)
    tryToClose(conn)
    return ReturnValue.OK


def removeRAMFromDisk(ramID: int, diskID: int) -> ReturnValue:
    message1 = f"""DELETE FROM {tablenames[RAM_AND_DISK]} 
            WHERE RAMID={ramID} AND DiskID={diskID};"""
    message2 = f"""UPDATE {tablenames[DISK_TABLE]} SET Freespace = Freespace + (SELECT Size FROM 
    {tablenames[RAM_TABLE]} WHERE RAMID = {ramID}) WHERE DiskID={diskID};"""
    print(message1 + message2)
    try:
        conn = Connector.DBConnector()
        rows, values = conn.execute(message1 + message2)
        conn.commit()
        if rows == 0:
            tryToClose(conn)
            return ReturnValue.NOT_EXISTS
    except DatabaseException as e:
        conn.rollback()
        tryToClose(conn)
        return ReturnValue.ERROR

    tryToClose(conn)
    return ReturnValue.OK


def averagePhotosSizeOnDisk(diskID: int) -> float:
    message=f"""SELECT AVG(DiskSizeNeeded) FROM PHOTOONDISK FULL JOIN PHOTO ON
                                PHOTOONDISK.PhotoID=Photo.PhotoID WHERE DiskID={diskID} GROUP BY DiskID"""
    try:
        conn = Connector.DBConnector()
        rows, values = conn.execute(message)
        conn.commit()

    except DatabaseException as e:
        conn.rollback()
        tryToClose(conn)
        return -1

    if rows == 0:
        tryToClose(conn)
        return 0

    tryToClose(conn)
    return values.rows[0][0]


def getTotalRamOnDisk(diskID: int) -> int:

    message=f"""SELECT SUM(Size) FROM RAMOnDisk FULL JOIN RAM ON
                                RAMOnDisk.RAMID=RAM.RAMID WHERE DiskID={diskID} GROUP BY DiskID"""
    try:
        conn = Connector.DBConnector()
        rows, values = conn.execute(message)
        conn.commit()

    except DatabaseException as e:
        conn.rollback()
        tryToClose(conn)
        return -1

    if rows == 0:
        tryToClose(conn)
        return 0

    tryToClose(conn)
    return values.rows[0][0]

def getCostForDescription(description: str) -> int:
    message = sql.SQL("""SELECT SUM(DiskSizeNeeded*CostPerByte) AS CostForDescription FROM 
    (SELECT Photo.PhotoID,DiskID,DiskSizeNeeded  FROM PhotoOnDisk FULL JOIN 
    Photo ON PhotoOnDisk.PhotoID = Photo.PhotoID WHERE
    Photo.Description = {d}) AS T1 FULL JOIN Disk ON
    T1.DiskID = Disk.DiskID;""").format(d=sql.Literal(description))

    try:
        conn = Connector.DBConnector()
        rows, values = conn.execute(message)
        conn.commit()

    except DatabaseException as e:
        conn.rollback()
        tryToClose(conn)
        return -1

    if values.rows[0][0] == None:
        tryToClose(conn)
        return 0

    tryToClose(conn)
    return values.rows[0][0]

def getPhotosCanBeAddedToDisk(diskID: int) -> List[int]:
    list = []
    message = sql.SQL("""SELECT PhotoID FROM Photo WHERE DiskSizeNeeded <= (SELECT FreeSpace 
    FROM Disk WHERE DiskID = {diskid}) ORDER BY PhotoID DESC LIMIT 5;""").format(diskid=sql.Literal(diskID))

    try:
        conn = Connector.DBConnector()
        rows, values = conn.execute(message)
        conn.commit()

    except DatabaseException as e:
        tryToClose(conn)
        return []

    for number in values.rows:
        list.append(number[0])

    tryToClose(conn)
    return list


def getPhotosCanBeAddedToDiskAndRAM(diskID: int) -> List[int]:
    list = []
    message = sql.SQL("""SELECT PhotoID FROM Photo WHERE DiskSizeNeeded <= (SELECT FreeSpace 
        FROM Disk WHERE DiskID = {id}) AND DiskSizeNeeded <= (SELECT SUM(Size) FROM RAMONDISK FULL JOIN
        RAM ON RAM.RAMID=RAMONDISK.RAMID WHERE RAMONDISK.DiskID={id}) 
        ORDER BY PhotoID ASC LIMIT 5;""").format(id=sql.Literal(diskID))

    try:
        conn = Connector.DBConnector()
        rows, values = conn.execute(message)
        conn.commit()

    except DatabaseException as e:
        tryToClose(conn)
        return []

    for number in values.rows:
        list.append(number[0])

    tryToClose(conn)
    return list


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
    conn = Connector.DBConnector()

    dropTables()
    createTables()
    photo =Photo(1, "'first'", 1)
    addPhoto(photo)
    #getPhotoByID(1)
    #deletePhoto(Photo(1, "'first'", 1))
    disk = Disk(1, "'ONE'", 10, 15, 2)
    addDisk(disk)

    #getDiskByID(1)
    #deleteRAM(1)
    photo2 = Photo(2, "'first'", 2)
    photo3 = Photo(3, "'first'", 2)
    photo4 = Photo(4, "'first'", 2)
    photo5 = Photo(5, "'first'", 2)
    photo6 = Photo(6, "'first'", 2)
    photo7 = Photo(7, "'first'", 2)
    disk2 = Disk(2, "'ONE'", 10, 4, 2)
    addPhoto(photo2)
    addPhoto(photo3)
    addPhoto(photo4)
    addPhoto(photo5)
    addPhoto(photo6)
    addPhoto(photo7)
    addDisk(disk2)

    #addPhotoToDisk(photo, 1)
    addPhotoToDisk(photo2, 2)
    addPhotoToDisk(photo, 2)

    ram = RAM(1, "'HELLO'", 5)
    addRAM(ram)
    addRAMToDisk(1, 1)


    #rows2, values2 = conn.execute(f"""SELECT PhotoID FROM Photo WHERE DiskSizeNeeded <= (SELECT SUM(Size)
    #FROM RAMONDISK FULL JOIN RAM ON RAM.RAMID=RAMONDISK.RAMID WHERE RAMONDISK.DiskID=1) AND DiskSizeNeeded(
    #SELECT FreeSpace FROM Disk WHERE DiskID = 1);""")

    #row, values = conn.execute(f"""SELECT FreeSpace FROM Disk WHERE DiskID = 1;""")


    print(getPhotosCanBeAddedToDiskAndRAM(1))

    #print(getCostForDescription("first1"))



    #print(averagePhotosSizeOnDisk(3))

    #removePhotoFromDisk(photo, 1)

    #removeRAMFromDisk(1, 2)
    tryToClose(conn)