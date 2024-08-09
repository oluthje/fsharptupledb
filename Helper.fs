module TestsHelpers

open FsharpTupleDB
open Types

let createTestRecordDescriptor (): Attr array =
    let attr1 = { Name = "EmpName"; Type = TypeVarChar; Length = 30 }
    let attr2 = { Name = "Age"; Type = TypeInt; Length = 4 }
    let attr3 = { Name = "Height"; Type = TypeReal; Length = 4 }
    let attr4 = { Name = "Salary"; Type = TypeInt; Length = 4 }

    [| attr1; attr2; attr3; attr4 |]

