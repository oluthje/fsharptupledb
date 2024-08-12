module Tests

open Xunit
open FsharpTupleDB
open System.IO
open PagedFileManagerModule
open RecordBasedFileManagerModule
open TestsHelpers
open Types

// pfm.fs tests
[<Fact>]
let ``Create File`` () =
    let pfm = PagedFileManager.Instance
    let filePath = "testfile.txt"
    File.Delete (filePath)

    pfm.createFile (filePath)
    Assert.True(File.Exists(filePath))

    File.Delete (filePath)

[<Fact>]
let ``Delete File`` () =
    let pfm = PagedFileManager.Instance
    let filePath = "testfile.txt"
    File.Delete (filePath)
    File.Create (filePath) |> ignore
    pfm.deleteFile (filePath) |> ignore
    Assert.True(not (File.Exists(filePath)))

// rbfm.fs tests

[<Fact>]
let ``Create File rbfm`` () =
    let rbfm = RecordBasedFileManager.Instance
    let filePath = "testfile.txt"
    File.Delete (filePath)

    rbfm.createFile(filePath)
    Assert.True(File.Exists(filePath))

    File.Delete (filePath)

[<Fact>]
let ``Insert Records`` () =
    let fileName = "testfile.txt"
    let rbfm = RecordBasedFileManager.Instance
    let fileHandle = FileHandle(fileName)
    File.Delete (fileName)

    // Create record based file
    rbfm.createFile(fileName)

    let count = 10
    let attributes = createTestRecordDescriptor()
    let inputRecords = createSmallRecords (count)
    let rids = Array.map (rbfm.insertRecord fileHandle attributes) inputRecords

    let outputRecords = Array.map (rbfm.readRecord fileHandle attributes) rids

    Assert.True(Array.forall2 compareTwoRecords inputRecords outputRecords)
    printfn "All %A records successfully inserted and read" count

    // Delete file
    File.Delete (fileName)

    ()

[<Fact>]
let ``Insert Many Records`` () =
    let fileName = "testfile.txt"
    let rbfm = RecordBasedFileManager.Instance
    let fileHandle = FileHandle(fileName)
    File.Delete (fileName)

    // Create record based file
    rbfm.createFile(fileName)

    let count = 1000
    let attributes = createTestRecordDescriptor()
    let inputRecords = createSmallRecords (count)
    let rids = Array.map (rbfm.insertRecord fileHandle attributes) inputRecords

    let outputRecords = Array.map (rbfm.readRecord fileHandle attributes) rids

    Assert.True(Array.forall2 compareTwoRecords inputRecords outputRecords)
    printfn "All %A records successfully inserted and read" count

    // Delete file
    File.Delete (fileName)

    ()

let updateRecordsTest (updateRecord: Value array -> Value array) =
    let fileName = "testfile.txt"
    let rbfm = RecordBasedFileManager.Instance
    let fileHandle = FileHandle(fileName)
    File.Delete (fileName)

    // Create record based file
    rbfm.createFile(fileName)

    let count = 1000
    let attributes = createTestRecordDescriptor()
    let inputRecords = createSmallRecords (count)
    let rids = Array.map (rbfm.insertRecord fileHandle attributes) inputRecords

    let updatedInputRecords = Array.map updateRecord inputRecords

    // update records
    Array.map2 (fun rid newRecord -> rbfm.updateRecord fileHandle attributes newRecord rid) rids updatedInputRecords |> ignore

    let outputRecords = Array.map (rbfm.readRecord fileHandle attributes) rids

    Assert.True(Array.forall2 compareTwoRecords updatedInputRecords outputRecords)
    printfn "All %A records successfully inserted, updated and read" count

    // Delete file
    File.Delete (fileName)

[<Fact>]
let ``Update Records to be same size`` () =
    let updateRecord (record: Value array) =
        let firstPartOfRecord = Array.sub record 0 (record.Length - 1)
        let newValue = Int 200
        Array.concat [firstPartOfRecord; [| newValue |]]
    updateRecordsTest updateRecord

[<Fact>]
let ``Update Records to be smaller`` () =
    let updateRecord (record: Value array) =
        let lastPartOfRecord = Array.sub record 1 (record.Length - 1)
        let newValue = String ""
        Array.concat [[| newValue |]; lastPartOfRecord]

    updateRecordsTest updateRecord

[<Fact>]
let ``Update Records to be larger`` () =
    let updateRecord (record: Value array) =
        let lastPartOfRecord = Array.sub record 1 (record.Length - 1)
        let newValue = String (String.replicate 4000 "a")
        Array.concat [[| newValue |]; lastPartOfRecord]

    updateRecordsTest updateRecord

[<Fact>]
let ``Delete Records`` () =
    let fileName = "testfile.txt"
    let rbfm = RecordBasedFileManager.Instance
    let fileHandle = FileHandle(fileName)
    File.Delete (fileName)

    // Create record based file
    rbfm.createFile(fileName)

    let count = 2
    let attributes = createTestRecordDescriptor()
    let inputRecords = createSmallRecords (count)
    let rids = Array.map (rbfm.insertRecord fileHandle attributes) inputRecords

    let outputRecords = Array.map (rbfm.readRecord fileHandle attributes) rids

    Assert.True(Array.forall2 compareTwoRecords inputRecords outputRecords)
    printfn "All %A records successfully inserted and read" count

    printfn "A"

    // Delete records
    Array.iter (fun rid -> rbfm.deleteRecord fileHandle rid) rids

    printfn "B"

    let outputRecords = Array.map (rbfm.readRecord fileHandle attributes) rids

    printfn "C"

    Assert.True(Array.forall (fun record -> record = [||]) outputRecords)
    printfn "All %A records successfully deleted" count

    // Delete file
    File.Delete (fileName)

    printfn "D"

    ()