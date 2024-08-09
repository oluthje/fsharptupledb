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