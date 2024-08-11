namespace FsharpTupleDB

open Types
open System

module PagedFileManagerModule =
    open System.IO

    type PageNum =
        int

    type PagedFileManager private () =
        // Private static mutable field to hold the instance
        static let mutable instance = None : PagedFileManager option

        // Public static member to get the instance
        static member Instance =
            match instance with
            | Some value -> value
            | None ->
                let value = PagedFileManager()
                instance <- Some value
                value

        member this.createFile (filePath: string) =
            if not (File.Exists(filePath)) then
                let res = File.Create(filePath)
                res.Close()
            else
                ()

        member this.deleteFile (filePath): Result<string, string> =
            try
                File.Delete(filePath)
                Ok "Successfully deleted file."
            with
                | ex -> Error (sprintf "An error occurred: %s" ex.Message)

    type FileHandle(filename: string) =

        let pfm = PagedFileManager.Instance

        member this.FileName = filename

        member this.readPage(pageNum: PageNum): byte array =
            let pageCount = this.getNumberOfPages()
            if pageNum <= pageCount then
                let page = Array.zeroCreate<byte> PAGESIZE
                use fileStream = new FileStream(this.FileName, FileMode.Open, FileAccess.Read)
                let pageOffset = int64 (pageNum * PAGESIZE)
                fileStream.Seek(pageOffset, SeekOrigin.Begin) |> ignore
                fileStream.Read(page, 0, PAGESIZE) |> ignore
                page
            else
                failwith "pageNum is greater than the total number of pages in the file."

        member this.writePage(pageNum: PageNum) (data: byte array) =
            let pagesInFile = this.getNumberOfPages()
            if (pageNum > pagesInFile) || not (data.Length = PAGESIZE) then
                failwith (sprintf "PageNum is greater than the total number of pages in the file, or data to write to page is not PAGESIZE in length. PageNum: %d, PagesInFile: %d, DataLength: %d" pageNum pagesInFile data.Length)
            else
                use fileStream = new FileStream(this.FileName, FileMode.Open, FileAccess.ReadWrite)
                let pageOffset = int64 (pageNum * PAGESIZE)
                fileStream.Seek(pageOffset, SeekOrigin.Begin) |> ignore

                fileStream.Write(data, 0, PAGESIZE)
                fileStream.Flush()  // Ensure all data is written

        member this.appendPage(data: byte array) =
            // let numPages = this.getNumberOfPages()
            // use fileStream = new FileStream(this.FileName, FileMode.Append, FileAccess.Write)
            // fileStream.Write(data, numPages * PAGESIZE, data.Length)
            // fileStream.Flush()  // Ensure all data is written
            let bytes = File.ReadAllBytes(this.FileName)
            let newBytes = Array.append bytes data
            File.WriteAllBytes(this.FileName, newBytes)

        member this.getNumberOfPages(): int =
            let bytes = File.ReadAllBytes(this.FileName)
            Math.Ceiling(float bytes.Length / float PAGESIZE) |> int
