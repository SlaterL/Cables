package main

import (
	"cables"
)

const port = 8082

func main() {
	server := cables.NewCablesServer(port)
	server.Serve()

}
