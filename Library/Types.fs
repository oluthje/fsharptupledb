namespace FsharpTupleDB

module Types =
    let PAGESIZE = 4096

    type AttrType =
        | TypeInt
        | TypeReal
        | TypeVarChar

    type AttrLength =
        int

    type Attr = {
        Name: string
        Type: AttrType
        Length: AttrLength
    }

    type Value =
        | Int of int
        | Float of float32
        | String of string
        | Null

    // Record ID. References record by page number in file, and slot number in slot directory
    type RID = {
        PageNum: int
        SlotNum: int
    }

    type SlotDirectoryHeader = {
        FreeSpaceOffset: int
        RecordEntriesNumber: int
    }