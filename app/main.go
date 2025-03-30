package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Uncomment this block to pass the first stage
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}
	for {
		// Accept connection
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleRequest(conn)
	}
}

func handleRequest(conn net.Conn) {
	defer conn.Close()

	// Read from client into buffer of max size 1024 Bytes
	buf := make([]byte, 1024)
	if _, err := conn.Read(buf); err != nil {
		fmt.Println("Error while reading the response.")
	}

	// Fetch request api key
	request_api_version := binary.BigEndian.Uint16(buf[4:6])
	error_code := []byte{0, 0}
	if request_api_version > 4 || request_api_version < 0 {
		error_code = []byte{0, 35}
	}

	// Fetch correlation id
	correlation_id := buf[8:12]

	// Create response
	response := []byte{0, 0, 0, 0}
	response = append(response, correlation_id...)
	response = append(response, error_code...)

	// Send response
	_, err := conn.Write(response)
	if err != nil {
		fmt.Println("Error sending response:", err)
	}
}
