# do
Some magic features implemented using golang's generics


### Contributing
You can commit PR to this repository

### How to get it?
````
go get -u github.com/gobkc/do
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