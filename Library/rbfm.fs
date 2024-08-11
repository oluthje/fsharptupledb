namespace FsharpTupleDB

open PagedFileManagerModule
open Types
open Serialization

module RecordBasedFileManagerModule =
    open System

    let toString (header: SlotDirectoryHeader) =
        // Convert integers to byte arrays
        let freeSpaceOffsetBytes = BitConverter.GetBytes(header.FreeSpaceOffset)
        let recordEntriesNumberBytes = BitConverter.GetBytes(header.RecordEntriesNumber)

        // Combine the byte arrays
        let combinedBytes = Array.concat [freeSpaceOffsetBytes; recordEntriesNumberBytes]

        // Convert the combined byte array to a base64 string
        Convert.ToBase64String(combinedBytes)

    let fromString (str: string) =
        // Convert the base64 string back to a byte array
        let combinedBytes = Convert.FromBase64String(str)

        // Extract the integers from the byte array
        let freeSpaceOffset = BitConverter.ToInt32(combinedBytes, 0)
        let recordEntriesNumber = BitConverter.ToInt32(combinedBytes, 4)

        // Create the SlotDirectoryHeader
        { FreeSpaceOffset = freeSpaceOffset; RecordEntriesNumber = recordEntriesNumber }

    let slotDirectoryHeaderToString (header: SlotDirectoryHeader) =
        let freeSpaceOffsetBytes = BitConverter.GetBytes(header.FreeSpaceOffset)
        let recordEntriesNumberBytes = BitConverter.GetBytes(header.RecordEntriesNumber)
        let combinedBytes = Array.concat [freeSpaceOffsetBytes; recordEntriesNumberBytes]
        Convert.ToBase64String(combinedBytes)

    type RecordBasedFileManager () =
        let pfm = PagedFileManager.Instance

        let newRecordBasedPage =
            // NumRecords and FreeSpaceOffset each start at 0 when there are no records
            let numRecordsBytes = BitConverter.GetBytes(0)
            let freeSpaceOffsetBytes = BitConverter.GetBytes(0)
            let emptyBytes = Array.zeroCreate<byte> (PAGESIZE - numRecordsBytes.Length - freeSpaceOffsetBytes.Length)
            Array.concat [emptyBytes; numRecordsBytes; freeSpaceOffsetBytes]

        let getNumberOfRecordsFromPage(page: byte array) = convertBytesToInt (Array.sub page (PAGESIZE - (4 * 2)) 4)
        let getFreeSpaceOffsetFromPage(page: byte array) = convertBytesToInt (Array.sub page (PAGESIZE - 4) 4)

        // find the current slot bytes in page and returns them
        let getSlotsBytes (page: byte array) (numRecords: int) =
            let slotsLength = numRecords * 4 * 2
            let slotsStartIndex = PAGESIZE - (slotsLength + 8)
            if slotsLength = 0 then
                [||]
            else
                Array.sub page slotsStartIndex slotsLength

        // calculate the position in the page where the slotIndex is located
        let calculateSlotIndex (slotNum) = PAGESIZE - ((slotNum * 8) + 8)

        let findRecordLength (slotNum: int) (page: byte array) =
            let slotIndex = calculateSlotIndex slotNum
            convertBytesToInt (Array.sub page (slotIndex + 4) 4)

        let findRecordOffset (slotNum: int) (page: byte array) =
            convertBytesToInt (Array.sub page (calculateSlotIndex slotNum) 4)

        // returns the number of free bytes available in page
        let freeSpaceInPage (page: byte array) =
            let slotDirectorySize = (getNumberOfRecordsFromPage(page) * 4 * 2) + 8
            PAGESIZE - (getFreeSpaceOffsetFromPage(page) + slotDirectorySize)

        let calculateNewFreeSpace (currentRecordsSize: int) (newRecordSize: int) (addedSlotSize: int) (currentSlotsBytes: int) =
            // also includes 4 bytes each for number of records and free space offset
            let slotDirectoryBytes = currentSlotsBytes + 4 + 4
            PAGESIZE - (currentRecordsSize + newRecordSize + addedSlotSize + slotDirectoryBytes)

        static let mutable instance = None : RecordBasedFileManager option

        // Public static member to get the instance
        static member Instance =
            match instance with
            | Some value -> value
            | None ->
                let value = RecordBasedFileManager()
                instance <- Some value
                value


        // for return type result, first type is return value if OK, second is return type if Error
        member this.createFile(filename) =
            pfm.createFile(filename)

        member this.destroyFile(filename) =
            pfm.deleteFile(filename)

        (*
        Page format is:
        |record_1|record_2|...|record_n|free space|record_n offset|record_n length|...|record_2 offset|record_2 length|record_1 offset|record_1 length|num_records|free space offset|
        *)
        member this.insertRecord(fileHandle: FileHandle) (recordDescriptor: Attr array) (values: Value array): RID =
            let serializedRecord = serializeRecord recordDescriptor values

            // check if page is 0, if it is, add a page
            if fileHandle.getNumberOfPages() = 0 then
                fileHandle.appendPage newRecordBasedPage

            // check if there is space available in page pageCount - 1
            let pageCount = fileHandle.getNumberOfPages()
            let currentPageNum = pageCount - 1
            let page = fileHandle.readPage(currentPageNum)
            let freeSpace = freeSpaceInPage page

            // check if record size <= freeSpace. If not, add a new page and continue on with that page
            let pageToAddRecord =
                let sizeOfRecordSlot = 8
                if serializedRecord.Length + sizeOfRecordSlot < freeSpace then
                    currentPageNum
                else
                    fileHandle.appendPage newRecordBasedPage
                    currentPageNum + 1

            // actually add record to page
            // get free space offset and number of records
            let (newPage, recordRID) =
                let page = fileHandle.readPage(pageToAddRecord)
                let numberOfRecords = getNumberOfRecordsFromPage(page)
                let freeSpaceOffset = getFreeSpaceOffsetFromPage(page)
                let newNumberOfRecordsBytes = convertIntToBytes (numberOfRecords + 1)
                let newFreeSpaceOffsetBytes = convertIntToBytes (freeSpaceOffset + serializedRecord.Length)

                // add record offset and record length to slot directory
                let recordOffsetBytes = convertIntToBytes(freeSpaceOffset)
                let recordLengthBytes = convertIntToBytes(serializedRecord.Length)
                let newSlot = Array.append recordOffsetBytes recordLengthBytes

                let recordRID = { PageNum = pageToAddRecord; SlotNum = numberOfRecords + 1 }

                let currentSlotBytes = getSlotsBytes page numberOfRecords

                // now write record, combined, and new free space offset and number of records number
                let (currentRecordsBytes, _) = Array.splitAt freeSpaceOffset page
                let freeSpace = calculateNewFreeSpace (currentRecordsBytes.Length) (serializedRecord.Length) (newSlot.Length) (currentSlotBytes.Length)
                let freeSpaceBytes = Array.zeroCreate freeSpace

                (Array.concat [currentRecordsBytes; serializedRecord; freeSpaceBytes; newSlot; currentSlotBytes; newNumberOfRecordsBytes; newFreeSpaceOffsetBytes;],
                    recordRID)

            fileHandle.writePage pageToAddRecord newPage
            recordRID

        // |record_1|record_2|...|record_n|free space|record_n offset|record_n length|...|record_2 offset|record_2 length|record_1 offset|record_1 length|num_records|free space offset|
        // this is really just a delete followed by an insert while maintaining the same RID. Need to implement forwarding if record grows in size
        member this.updateRecord(fileHandle: FileHandle) (recordDescriptor: Attr array) (values: Value array) (rid: RID) =
            let page = fileHandle.readPage(rid.PageNum)
            let numberOfRecords = getNumberOfRecordsFromPage(page)
            let freeSpaceOffset = getFreeSpaceOffsetFromPage(page)
            let serializedRecord = serializeRecord recordDescriptor values
            let previousRecordLength = findRecordLength rid.SlotNum page

            let recordFitsInPage =
                let freeSpace = freeSpaceInPage page
                serializedRecord.Length <= previousRecordLength || freeSpace - 8 - serializedRecord.Length >= 0

            let recordOffset = findRecordOffset rid.SlotNum page
            let recordLength = findRecordLength rid.SlotNum page

            // printfn "old size: %A" previousRecordLength
            // printfn "new length: %A" serializedRecord.Length

            // case 1: record shrinks, stays the same size, or grows but fits in page
            //  -compact following records such that record is removed
            //  -reinsert record at the end of the old records
            //  -update freeSpaceOffset, slot directory: record length and record offset
            let updateRecordInPage () =
                // let recordOffset = findRecordOffset rid.SlotNum page
                // let recordLength = findRecordLength rid.SlotNum page
                let recordsBeforeRecord = Array.sub page 0 recordOffset
                let recordsAfterRecord = Array.sub page (recordOffset + recordLength) (freeSpaceOffset - recordsBeforeRecord.Length - recordLength)
                let currentSlotBytes = getSlotsBytes page numberOfRecords
                let slots = Array.chunkBySize 8 currentSlotBytes

                // if not recordToUpdate, decrease offset by oldRecordSize
                // else set new offset and length
                let updateSlots (index: int) (slot: byte array) =
                    let slotNum = numberOfRecords - index
                    if slotNum = rid.SlotNum then
                        let newOffset = recordsBeforeRecord.Length + recordsAfterRecord.Length
                        let newLength = serializedRecord.Length
                        Array.concat [convertIntToBytes newOffset; convertIntToBytes newLength]
                    else
                        let newOffset = convertBytesToInt (Array.sub slot 0 4) - previousRecordLength
                        let oldLength = Array.sub slot 4 4
                        Array.concat [convertIntToBytes newOffset; oldLength]
                let newSlotBytes = Array.fold (fun bytes -> Array.append bytes) [||] (Array.mapi updateSlots slots)
                // for each old slot that is not rid.SlotNum, decrease slot offset by oldRecordSize

                let newFreeSpaceOffset = freeSpaceOffset - (previousRecordLength - serializedRecord.Length)
                let freeSpace = calculateNewFreeSpace (recordsBeforeRecord.Length + recordsAfterRecord.Length) (serializedRecord.Length) (0) (newSlotBytes.Length)
                let newPage = Array.concat [recordsBeforeRecord; recordsAfterRecord; serializedRecord; (Array.zeroCreate freeSpace); newSlotBytes; convertIntToBytes numberOfRecords; convertIntToBytes newFreeSpaceOffset]
                fileHandle.writePage rid.PageNum newPage

            // case 1: record stays the same size
            if serializedRecord.Length = previousRecordLength then
                updateRecordInPage()
            else if serializedRecord.Length < previousRecordLength then
                updateRecordInPage()
            else
                if recordFitsInPage then
                    updateRecordInPage()
                else
                    // let recordOffset = findRecordOffset rid.SlotNum page
                    // let recordLength = findRecordLength rid.SlotNum page
                    // remove old record
                    let recordsBeforeRecord = Array.sub page 0 recordOffset
                    printfn "startIndex: %A" (recordOffset + recordLength)
                    printfn "freeSpaceOffset: %A" freeSpaceOffset
                    printfn "recordsBeforeRecord: %A" recordsBeforeRecord.Length
                    printfn "recordLength: %A" recordLength
                    printfn "count: %A" (freeSpaceOffset - recordsBeforeRecord.Length - recordLength)
                    let recordsAfterRecord = Array.sub page (recordOffset + recordLength) (freeSpaceOffset - recordsBeforeRecord.Length - recordLength)
                    // need to do forwarding here.
                    let ridForForwarding = this.insertRecord fileHandle recordDescriptor values
                    // find page with free space available
                    // really just a call to insertRecord
                    // update slot directory
                    let currentSlotBytes = getSlotsBytes page numberOfRecords
                    let slots = Array.chunkBySize 8 currentSlotBytes

                    // if recordToUpdate, set forwarding data. In offset put PageNum, in length put SlotNum
                    let updateSlots (index: int) (slot: byte array) =
                        let slotNum = numberOfRecords - index
                        if slotNum = rid.SlotNum then
                            let newOffset = -ridForForwarding.PageNum
                            let newLength = ridForForwarding.SlotNum
                            Array.concat [convertIntToBytes newOffset; convertIntToBytes newLength]
                        else
                            let newOffset = convertBytesToInt (Array.sub slot 0 4) - previousRecordLength
                            let oldLength = Array.sub slot 4 4
                            Array.concat [convertIntToBytes newOffset; oldLength]

                    let newSlotBytes = Array.fold (fun bytes -> Array.append bytes) [||] (Array.mapi updateSlots slots)

                    let newFreeSpaceOffset = freeSpaceOffset - serializedRecord.Length
                    let freeSpace = calculateNewFreeSpace (recordsBeforeRecord.Length + recordsAfterRecord.Length) (0) (0) (newSlotBytes.Length)
                    let newPage = Array.concat [recordsBeforeRecord; recordsAfterRecord; (Array.zeroCreate freeSpace); newSlotBytes; convertIntToBytes numberOfRecords; convertIntToBytes newFreeSpaceOffset]
                    fileHandle.writePage rid.PageNum newPage



            // case 3: record increased in size
            // -if there is sufficient space in the page, simply follow steps under case 2
            // -if no space, then put record in next available page. Set old slot to point to the page where record has been moved to


            ()

        member this.readRecord(fileHandle: FileHandle) (recordDescriptor: Attr array) (rid: RID): Value array =
            let page = fileHandle.readPage(rid.PageNum)

            // get record offset and record length
            let recordOffset = findRecordOffset rid.SlotNum page
            let recordLength = findRecordLength rid.SlotNum page

            if recordOffset < 0 then
                let forwardingRID = { PageNum = -recordOffset; SlotNum = recordLength }
                this.readRecord fileHandle recordDescriptor rid
            else
                let serializedRecord = Array.sub page recordOffset recordLength
                deserializeRecord recordDescriptor serializedRecord

        member this.printRecord(recordDescriptor: Attr array) (record: Value array) =
            printfn "--------------"
            let printValues (values: Value array) =
                for value in values do
                    match value with
                    | Int i -> printfn "%d" i
                    | Float f -> printfn "%f" f
                    | String s -> printfn "%s" s
                    | Null -> printfn "Null"

            printValues record