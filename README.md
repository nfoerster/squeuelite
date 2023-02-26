# squeuelite - a persistent queue based on SQLite writteni in Go

[![Go](https://github.com/nfoerster/squeuelite/actions/workflows/go.yml/badge.svg)](https://github.com/nfoerster/squeuelite/actions/workflows/go.yml)

## description

SQueueLite is a persistant queue based on SQLite. It uses a reliable persistent file-based battle-tested solution known as [SQLite](https://www.sqlite.org/).

## usage

### subscriber mode

```golang
package squeuelite

//main routine
q, err := squeuelite.NewPQueue("test.db", 10)
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
payload := []byte("Payload")
err := q.Put(payload)
if err != nil {
    t.Error(err)
}
```

### manual mode

```golang
package squeuelite

//main routine
q, err := squeuelite.NewPQueue("test.db", 10)
if err != nil {
    return err
}
defer q.Close()

//put routine
payload := []byte("Payload")
err := q.Put(payload)
if err != nil {
    t.Error(err)
}

//put routine
m, err := q.Peek()
if err != nil {
    t.Error(err)
}
err = processMsg(m.Data) //your process function
if err != nil{
    errint := q.MarkFailed(m.MessageID)
    if errint != nil{
        //handle db errors setting msg to fail
    }
} else {
    errint := q.Done(m.MessageID)
    if errint != nil{
        //handle db errors deleting completed msg
    }
}

```

## contributors

* [Bernd Lunghamer](https://github.com/blunghamer)

## related projects

Inital code was taken from https://github.com/litements/litequeue, which offers a persistent queue using SQLite in Python, translated to Go and adapted accordingly.
