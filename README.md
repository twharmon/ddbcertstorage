# DDBCertStorage
DynamoDB storage for CertMagic.

## Usage
```go
var err error
certmagic.Default.Storage, err = ddbcertstorage.New("TableName")
if err != nil {
    log.Fatalln(err)
}
```
