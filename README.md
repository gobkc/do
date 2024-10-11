# do
Some magic features implemented using golang's generics


### Contributing
You can commit PR to this repository

### How to get it?
````
go get -u github.com/gobkc/do
````

### Unit test?

1. how to test all unit test cases?

````
go test -v
````

2. how to test a specific unit test case?

````
go test -v -run Poller
````

### Quick start
````
package main

import (
	"github.com/gobkc/do"
	"log"
)

func main() {
    a := 1
    log.Println(do.OneOf(a==1,true,false))
}
````

result : true

### License
© Gobkc, 2023~time.Now

Released under the Apache License