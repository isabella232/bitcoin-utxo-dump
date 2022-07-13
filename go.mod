module utxodump

go 1.18

require (
	github.com/akamensky/base58 v0.0.0-20210829145138-ce8bf8802e8f
	github.com/in3rsha/bitcoin-utxo-dump v0.0.0-00010101000000-000000000000
	github.com/syndtr/goleveldb v1.0.0
	github.com/xitongsys/parquet-go v1.6.2
	golang.org/x/crypto v0.0.0-20220622213112-05595931fe9d
)

replace github.com/in3rsha/bitcoin-utxo-dump => ./
