# seeefs
Google drive mount tool optimized for seeding.

## What's optimized

This tool divide big files into small blocks, and combine small files into big blocks. Then the filesystem structures and information of blocks are saved locally. When the directory is read, it caches the blocks.

Also, this tool supports multiple google accounts for transferring.

## How to use
First edit the following line in `backend/backend.go`, change it to your root folder id.

```go
const ROOT_FOLDER = "your root folder id"
```

Then run `go run main.go drive addtoken` to add google drive accounts which uploads and downloads files. (Make sure every account has permission to write the root folder)

Other commands are:

`go run main.go mount` to mount the filesystem using FUSE with readonly.

`go run main.go copy SOURCE DESTINATION` to copy some files from `SOURCE` to `DESTINATION`.

## Configuration

You can edit configuration in `main.go` and `backend/backend.go` (~~I was too lazy to put in config files~~).

## Other online drives

Just edit `backend/backend.go`, it should be not very difficult to change to other drives.

(I chose google just because its size is unlimited ~~if you payed gsuite or using educational edition~~)

## Plans

- Optimize the transfer to google drive, to earlier download the pieces (of blocks) which is required earlier.

- ~~Directly writing support.~~
