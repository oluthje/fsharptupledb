namespace FsharpTupleDB

open System
open Types

module Serialization =
    open System.Text

    // Sets a bit at bitIndex in byte
    let setBit (byte: byte) (bitIndex: int) (value: bool) : byte =
        if value then
            byte ||| (1uy <<< bitIndex)
        else
            byte &&& (~~~(1uy <<< bitIndex))

    // gets the bit at bitIndex
    let getBit (byte: byte) (bitIndex: int) : bool =
        (byte &&& (1uy <<< bitIndex)) <> 0uy

    // Function to create a byte array where each bit in each byte is set to a value
    let createByteArray (size: int) (bitValues: bool[]) : byte[] =
        // let byteArray = Array.zeroCreate<byte> 1
        // for i in 0 .. 7 do
        //     byteArray.[0] <- setBit byteArray.[0] i bitValues.[i]
        // byteArray
        Array.zeroCreate<byte> 1

    // reverse of the above function
    let readByteArray (byteArray: byte[]) : bool[] =
        // let bitValues = Array.zeroCreate<bool> 1
        // for i in 0 .. 7 do
        //     bitValues.[i] <- getBit byteArray.[0] i
        // bitValues
        Array.zeroCreate<bool> 1

    let getStringAtOffset (offset: int) (record: byte array) =
        let firstFourBytes = Array.sub record offset 4
        let length = BitConverter.ToInt32(firstFourBytes)
        let bytes = Array.sub record (offset + 4) length

        let encoding = Encoding.UTF8
        let string = encoding.GetString(bytes)
        String string

    let isNull (value: Value) =
        match value with
        | Null -> true
        | _ -> false

    let convertBytesToInt (bytes: byte array) = BitConverter.ToInt32(bytes)

    let convertIntToBytes (int: int) = BitConverter.GetBytes(int)

    let convertValueToBytes (value: Value): byte array =
        match value with
        | Int x -> BitConverter.GetBytes(x)
        | Float x -> BitConverter.GetBytes(x)
        | String str -> Array.append (BitConverter.GetBytes(str.Length)) (System.Text.Encoding.ASCII.GetBytes(str))
        | Null -> [||]

    let convertBytesToValue (attrType: AttrType) (bytes: byte array) : Value =
        match attrType with
        | TypeVarChar -> getStringAtOffset 0 bytes
        | TypeInt -> Int (BitConverter.ToInt32(bytes))
        | TypeReal -> Float (BitConverter.ToSingle(bytes))

    (*
        attributes is list of attributes of record, with each attribute's corresponding
        value in values. Converts values to record byte data using attributes.
        Record structure is |num_attrs|null_flags|end_offset_1|end_offset_2|...|end_offset_|f_1|f_2|...|f_n|
    *)
    let serializeRecord (attributes: Attr array) (values: Value array): byte array =
        let rec getSizeOfField (value: Value): int =
            match value with
            | Int _ -> 4
            | Float _ -> 4
            | String x -> x.Length + getSizeOfField (Int 0)
            | Null -> 0

        // Step 1: count number of attributes
        let numAttrs = attributes.Length

        // Step 2: create byte array containing null flags. There are Ceiling(numAttributes / 8) bytes
        let bitValues = Array.map isNull values
        let nullFlags = createByteArray numAttrs bitValues

        // Step 3: get sizes of each field to store field offsets
        let fieldSizes = Array.map getSizeOfField values
        let bytesForOffsets = attributes.Length * 4
        let initialFieldOffset = getSizeOfField (Int 0) + nullFlags.Length + bytesForOffsets
        let offsets =
            match Array.mapFold (fun offset size -> (offset, offset + size)) initialFieldOffset fieldSizes with
            | (offsets, _) -> offsets

        // Step 4: convert all arrays to bytes and append them together to get serialized record
        let numAttrsAsByte = BitConverter.GetBytes(numAttrs)
        let convertIntToBytes (int: int) =
            BitConverter.GetBytes(int)

        // convert offsets to bytes
        let offsetsAsBytesArray = Array.map convertIntToBytes offsets
        let offsetsAsBytes = Array.fold (fun bytes -> Array.append bytes) [||] offsetsAsBytesArray

        // convert fields to bytes
        let arrayOfValuesAsBytes = Array.map convertValueToBytes values
        let fieldsAsBytes = Array.fold (fun bytes -> Array.append bytes) [||] arrayOfValuesAsBytes

        Array.concat [| numAttrsAsByte; nullFlags; offsetsAsBytes; fieldsAsBytes |]

    let deserializeRecord (attributes: Attr array) (record: byte array): Value array =
        // function to take attr and offset and get Value using record

        // !!! todo: refactor this function out. Should be able to be done with other functions above
        let getValueAtOffset (attribute: Attr) (offset: int) (record: byte array) : Value =
            let firstFourBytes = Array.sub record offset 4
            let someValue =
                match attribute.Type with
                | TypeVarChar -> getStringAtOffset offset record
                | TypeInt -> Int (BitConverter.ToInt32(firstFourBytes))
                | TypeReal -> Float (BitConverter.ToSingle(firstFourBytes))
            someValue

        // Step 1: Get number of attributes
        let numAttributes = attributes.Length

        // Step 2: Get null flags from serialized record
        let lengthOfNullFlags: int = Math.Ceiling(float numAttributes / 8.0) |> int
        // !!! To do: use null flags later when null values matter. Currently not taken into account
        let nullFlags = readByteArray (Array.sub record 4 lengthOfNullFlags)

        // Step 3: Get field offsets from serialized record
        let offsetsBytes = Array.sub record (4 + lengthOfNullFlags) (4 * numAttributes)
        let offsetsMid = Array.chunkBySize 4 offsetsBytes
        let attr = { Name = ""; Type = TypeInt; Length = 0 }

        let convertValueToInt (value: Value): int =
            match value with
            | Int x -> x
            | _ -> 0

        let offsets = Array.map convertValueToInt (Array.map (getValueAtOffset attr 0) offsetsMid)

        // Step 4: Use offsets to find values and return them
        Array.map2 (fun attr offset -> getValueAtOffset attr offset record) attributes offsets
