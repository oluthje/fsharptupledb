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

    pfm.createFile (filePath) |> ignore
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

    rbfm.createFile(filePath) |> ignore
    Assert.True(File.Exists(filePath))

    File.Delete (filePath)

[<Fact>]
let ``Insert Record`` () =
    let fileName = "testfile.txt"
    let attributes = createTestRecordDescriptor()
    let name = "John Doe"
    printfn "%d" name.Length
    let testRecord: Value array = [| String "John Doe"; Int 30; Float 5.9f; Int 10000 |]

    let rbfm = RecordBasedFileManager.Instance
    let result = rbfm.insertRecord fileName attributes testRecord

    ()