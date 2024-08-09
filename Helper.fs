module TestsHelpers

open FsharpTupleDB
open Types
open FsharpTupleDB.RecordBasedFileManagerModule

let createTestRecordDescriptor (): Attr array =
    let attr1 = { Name = "EmpName"; Type = TypeVarChar; Length = 30 }
    let attr2 = { Name = "Age"; Type = TypeInt; Length = 4 }
    let attr3 = { Name = "Height"; Type = TypeReal; Length = 4 }
    let attr4 = { Name = "Salary"; Type = TypeInt; Length = 4 }

    [| attr1; attr2; attr3; attr4 |]

let createSmallRecords (count: int) =
    let createRecord (i: int) =
        [| String (String.replicate i "Name"); Int i; Float (float32 i * 0.2f); Int (100*i) |]
    Array.map createRecord [| for i in 1 .. count -> i |]

let compareTwoRecords (record1: Value array) (record2: Value array) =
    let printValues (values: Value array) =
        for value in values do
            match value with
            | Int i -> printfn "%d" i
            | Float f -> printfn "%f" f
            | String s -> printfn "%s" s
            | Null -> printfn "Null"

    let result =
        Array.forall2 (fun value1 value2 ->
            match value1, value2 with
            | Int x, Int y -> x = y
            | Float x, Float y -> x = y
            | String x, String y -> x = y
            | Null, Null -> true
            | _ -> false
        ) record1 record2
    result