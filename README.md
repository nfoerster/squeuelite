# squeuelite - a persistent queue based on SQLite writteni in Go

[![Go](https://github.com/nfoerster/squeuelite/actions/workflows/go.yml/badge.svg)](https://github.com/nfoerster/squeuelite/actions/workflows/go.yml)

## description

SQueueLite is a persistant queue based on SQLite. It uses a reliable persistent file-based battle-tested solution known as [SQLite](https://www.sqlite.org/).

## usage

### subscriber mode

```golang
package squeuelite

//main routine
q, err := squeuelite.NewSQueue("test.db")
if err != nil {
    return err
}
defer q.Close()

err = q.Subscribe(func(m *PMessage) error {
        log.Printf("message received:%v",m.Data)
        errint := processMsg(m.Data) //your process function
        if errint != nil{
            //message will be marked as failed and not again delivered
            return err
        }
        //message will be deleted from persistent storage
		return nil
	})
if err != nil {
    return err
}

//put routine
go func() {
    payload := []byte("Payload")
    err := q.Put(payload)
    if err != nil {
        t.Error(err)
    }
}()
```


## contributors

* [Bernd Lunghamer](https://github.com/blunghamer)

## related projects

Inital code was taken from https://github.com/litements/litequeue, which offers a persistent queue using SQLite in Python, translated to Go and adapted accordingly.
